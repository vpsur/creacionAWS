import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import os
import time

load_dotenv()

def conectarse():
    session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))

    ec2 = session.client('ec2')
    efs = session.client('efs')

    response = ec2.describe_instances()
    print(response)
    return ec2, efs

def crear_instanciaEC2(ec2):
    INSTANCE_NAME = os.getenv("EC2_INSTANCE_NAME")
    IMAGE_ID = os.getenv("AMI_ID")
    INSTANCE_TYPE = "t2.micro"

    #Buscar que exista la instancia
    try:
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': [INSTANCE_NAME]},
                {'Name': 'instance-state-name', 'Values': ['pending', 'running']}
            ]
        )

        instances = [i for r in response['Reservations'] for i in r['Instances']]
        if instances:
            print("Instancia ya existe")
            instance_id = instances[0]['InstanceId']
            return instance_id

    except ClientError as e:
        print("Error al buscar instancias:", e)
    
    # Crear nueva instancia si no existe
    print("Creando nueva instancia EC2")
    instances = ec2.run_instances(
        ImageId=IMAGE_ID,
        InstanceType=INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        KeyName=os.getenv("KEY_PAIR_NAME"),
        SecurityGroupIds=[os.getenv("SECURITY_GROUP_ID")],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': INSTANCE_NAME}]
        }]
    )

    instance_id = instances['Instances'][0]['InstanceId']
    print(f"Instancia creada: {instance_id}")

    waiter = ec2.get_waiter('instance_running')
    print("Esperando a que la instancia esté corriendo...")
    waiter.wait(InstanceIds=[instance_id])
    print("Instancia ahora está corriendo")

    return instance_id

def pararinstancia(ec2, instance_id):
    ec2.stop_instances(InstanceIds=[instance_id])
    print("Parando la instancia")

    
def eliminarinstancia(ec2, instance_id):
    ec2.terminate_instances(InstanceIds=[instance_id])
    print("Terminando la instancia")


def crearEBS(ec2, instance_id):

    size_gb=10
    device_name='/dev/sdf'
    
    #Crear el volumen EBS en la misma zona que la instancia
    instance_desc = ec2.describe_instances(InstanceIds=[instance_id])
    availability_zone = instance_desc['Reservations'][0]['Instances'][0]['Placement']['AvailabilityZone']
    print(f"Creando volumen de {size_gb}GB en {availability_zone}...")
    volume = ec2.create_volume(
        AvailabilityZone=availability_zone,
        Size=size_gb,
        VolumeType='gp3',
        TagSpecifications=[{
            'ResourceType': 'volume',
            'Tags': [{'Key': 'Name', 'Value': f'Volu-{instance_id}'}]
        }]
    )
    volume_id = volume['VolumeId']
    print(f"Volumen creado: {volume_id}")

    waiter = ec2.get_waiter('volume_available')
    waiter.wait(VolumeIds=[volume_id])
    print("Volumen listo")

    #Adjuntar el volumen
    print(f"Adjuntando volumen {volume_id} a la instancia {instance_id}...")
    ec2.attach_volume(
        VolumeId=volume_id,
        InstanceId=instance_id,
        Device=device_name
    )
    print("Volumen adjuntado")

    response = ec2.describe_instances(InstanceIds=[instance_id])
    instance = response['Reservations'][0]['Instances'][0]

    instance_ip = instance.get('PublicIpAddress')
    key_path = os.getenv("key_path")
    linux_device = "/dev/xvdf"
    mount_point = "/mnt/datos"

    # Comando SSH para formatear y montar
    cmd = f"""
    ssh -o StrictHostKeyChecking=no -i {key_path} ec2-user@{instance_ip} \\
    '    sudo mkfs -t ext4 {linux_device} && \
        sudo mkdir -p {mount_point} && \
        sudo mount {linux_device} {mount_point} && \
        sudo chown ec2-user:ec2-user {mount_point} '
    """

    os.system(cmd)
    print("Volumen montado")

    #Subir el archivo
    archivo_local = "archivo.txt"
    cmd_scp = f"scp -i {key_path} {archivo_local} ec2-user@{instance_ip}:{mount_point}/"
    os.system(cmd_scp)
    print(f"Archivo {archivo_local} copiado a {mount_point}")

def crearEFS(ec2, instance_id, efs):
    #Crear instancia
    try:
        response = efs.create_file_system(
            CreationToken=f"efs-{instance_id}",
            Tags=[{'Key': 'Name', 'Value': 'MiEFS'}]
        )
        fs_id = response['FileSystemId']
        print(f"Éxito: Se ha creado el EFS con ID {fs_id}")
    except efs.exceptions.FileSystemAlreadyExists:
        # Si ya existe, simplemente lo buscamos para obtener su ID
        response = efs.describe_file_systems(CreationToken=f"efs-{instance_id}")
        fs_id = response['FileSystems'][0]['FileSystemId']
        print(f"El EFS ya existía: {fs_id}")

    #Crear datos desde la instancia
    instance = ec2.describe_instances(InstanceIds=[instance_id])
    instance = instance['Reservations'][0]['Instances'][0]

    subnet_id = instance['SubnetId']
    security_group_id = instance['SecurityGroups'][0]['GroupId']

    while True:
        desc = efs.describe_file_systems(FileSystemId=fs_id)
        if desc['FileSystems'][0]['LifeCycleState'] == 'available':
            break
        time.sleep(5)

    efs_dns = f"{fs_id}.efs.{os.getenv("REGION")}.amazonaws.com"

    efs.create_mount_target(
    FileSystemId=fs_id,
    SubnetId=subnet_id,
    SecurityGroups=[security_group_id]
    )

    print("Mount target creado")

    instance_ip = instance['PublicIpAddress']
    key_path = os.getenv("key_path")
    mount_point = "/mnt/efs"

    print("Vamo a mirarlo")
    print(fs_id)
    cmd_mount = f"""
        ssh -o StrictHostKeyChecking=no -i {key_path} ec2-user@{instance_ip} \\
        'sudo yum install -y amazon-efs-utils && \
        sudo mkdir -p {mount_point} && \
        sudo mount -t efs {fs_id}:/ {mount_point} && \
        sudo chown -R ec2-user:ec2-user {mount_point}'
    """
    os.system(cmd_mount)

    print("EFS montado")

    archivo_local = "archivo.txt"
    cmd_scp = f"scp -i {key_path} {archivo_local} ec2-user@{instance_ip}:{mount_point}/"
    os.system(cmd_scp)

    print(f"Archivo {archivo_local} copiado a {mount_point}")

def main():
    ec2, efs =conectarse()
    instance_id = crear_instanciaEC2(ec2)
    #pararinstancia(ec2, instance_id)
    #eliminarinstancia(ec2, instance_id)
    #crearEBS(ec2, instance_id)
    crearEFS(ec2, instance_id, efs)



if __name__ == "__main__":
    main()