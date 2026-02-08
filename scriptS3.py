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

    s3 = session.client('s3')

    response = s3.list_buckets()
    print(response)
    return s3


def bucket_standard(s3):
    #Crear el bucket
    bucket_name = "bucketstandard685"
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket creado: {bucket_name}")
    except ClientError as e:
        print(e)
    
    #Crear Carpetas
    carpetas = [
        "datos/",
        "datos/ventas/"
        "resultado/"
    ]

    for carpeta in carpetas:
        s3.put_object(Bucket=bucket_name, Key=carpeta)
        print(f"Carpeta creada: {carpeta}")
    
    #subir csv
    s3.upload_file(
        "ventas.csv",
        bucket_name,
        "datos/ventas/ventas.csv"
    )
    print("Archivo CSV subido a S3")

    #Obtener el csv
    s3.download_file(
        bucket_name,
        "datos/ventas/ventas.csv",
        "destino/ventas_descargado.csv"
    )
    print("Archivo CSV descargado desde S3")

def bucket_standard_ia(s3):
    #Crear el bucket
    bucket_name = "bucketstandardia685"
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket IA creado: {bucket_name}")
    except ClientError as e:
        print(e)
    
    #subir csv
    s3.upload_file(
        "ventas.csv",
        bucket_name,
        "ventas.csv",
        ExtraArgs={"StorageClass": "STANDARD_IA"}
    )
    print("Archivo CSV subido a S3 clase STANDARD_IA")

    #Obtener el csv
    s3.download_file(
        bucket_name,
        "ventas.csv",
        "destino/ventas_descargado_ia.csv"
    )
    print("Archivo CSV descargado desde S3 con clase STANDARD_IA")

def bucket_intelligent_tierning(s3):
    
    #Crear el bucket
    bucket_name = "bucketintelligenttierning685"
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket INTELLIGENT TIERING creado: {bucket_name}")
    except ClientError as e:
        print(e)
    
    #subir csv
    s3.upload_file(
        "ventas.csv",
        bucket_name,
        "ventas.csv",
        ExtraArgs={"StorageClass": "INTELLIGENT_TIERING"}
    )
    print("Archivo CSV subido a S3 clase INTELLIGENT_TIERING")

    #Obtener el csv
    s3.download_file(
        bucket_name,
        "ventas.csv",
        "destino/ventas_descargado_INTELLIGENT_TIERING.csv"
    )
    print("Archivo CSV descargado desde S3 con clase INTELLIGENT_TIERING")

def bucket_glacier(s3):

    #Crear Bucket
    bucket_name = "buckeglacier685"
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket Glacier creado: {bucket_name}")
    except ClientError as e:
        print(e)
    
    #subir el csv
    s3.upload_file(
        "ventas.csv",
        bucket_name,
        "ventas.csv",
        ExtraArgs={"StorageClass": "GLACIER"}
    )
    print("CSV subido a Glacier")

    #Restaurar Objeto
    s3.restore_object(
        Bucket=bucket_name,
        Key="ventas.csv",
        RestoreRequest={
            "Days": 1,
            "GlacierJobParameters": {
                "Tier": "Standard"
            }
        }
    )
    print("Solicitud de restauración enviada (Glacier)")

    

def descargar_desde_Glacier(s3):
    bucket_name = "buckeglacier685"
    #Hace falta restaurar el objeto lo que puede tardar mucho rato
    s3.download_file(
        bucket_name,
        "ventas.csv",
        "destino/ventas_glacier_descargado.csv"
    )
    print("CSV Glacier descargado")

def bucket_deep_archive(s3):
    #Crear bucket
    bucket_name = "buckeglacierdeep685"
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket Deep Archive creado: {bucket_name}")
    except ClientError as e:
        print(e)
    
    #Subir Archivo
    s3.upload_file(
        "ventas.csv",
        bucket_name,
        "ventas.csv",
        ExtraArgs={"StorageClass": "DEEP_ARCHIVE"}
    )
    print("CSV subido a Glacier Deep Archive")

    #Restaurar
    s3.restore_object(
        Bucket=bucket_name,
        Key="ventas.csv",
        RestoreRequest={
            "Days": 1,
            "GlacierJobParameters": {
                "Tier": "Bulk"
            }
        }
    )
    print("Solicitud de restauración enviada (Deep Archive)")

def descargar_desde_Glacier_deep(s3):
    bucket_name = "buckeglacierdeep685"
    #Hace falta restaurar el objeto lo que puede tardar mucho rato
    s3.download_file(
        bucket_name,
        "ventas.csv",
        "destino/ventas_glacier_deep_descargado.csv"
    )
    print("CSV Glacier Deep descargado")

def versionado(s3):
    bucket_name = "bucketstandard685"
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
            "Status": "Enabled"
        }
    )
    print(f"Versionado habilitado en {bucket_name}")

    with open("ventas.csv", "w") as f:
        f.write(
            "id,producto,cantidad,precio\n"
            "1,teclado,2,50\n"
        )

    s3.upload_file(
        "ventas.csv",
        bucket_name,
        "datos/ventas/ventas.csv"
    )

    print("Versión 1 subida")


    with open("ventas.csv", "a") as f:
        f.write(
            "2,mouse,5,20\n"
        )

    s3.upload_file(
        "ventas.csv",
        bucket_name,
        "datos/ventas/ventas.csv"
    )

    print("Versión 2 subida")

    response = s3.list_object_versions(
        Bucket=bucket_name,
        Prefix="datos/ventas/ventas.csv"
    )

    print("Versiones encontradas:")
    for v in response.get("Versions", []):
        print(
            f"VersionId: {v['VersionId']} | "
            f"IsLatest: {v['IsLatest']}"
        )
def main():
    s3 = conectarse()
    #bucket_standard(s3)
    #bucket_standard_ia(s3)
    #bucket_intelligent_tierning(s3)
    #bucket_glacier(s3)
    #descargar_desde_Glacier(s3)
    #bucket_deep_archive(s3)
    #descargar_desde_Glacier_deep(s3)
    versionado(s3)


if __name__ == "__main__":
    main()