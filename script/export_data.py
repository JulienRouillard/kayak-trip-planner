#Exporter les données dans un datalake et datawarehouse

#Librairies
import pandas as pd
import boto3
from pathlib import Path
from sqlalchemy import create_engine
from io import StringIO

#Configuration AWS
AWS_REGION = 'YOUR_AWS_REGION'
BUCKET_NAME = 'YOUR_BUCKET_NAME'
AWS_ACCESS_KEY_ID = "YOUR_AWS_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_KEY"

#Configuration MYSQL
DB_HOST = "YOUR_DB_ENDPOINT"
DB_USER = "admin"
DB_PASSWORD = "YOUR_DB_PASSWORD"
DB_NAME = "YOUR_DB_NAME"

#Chemin des dossiers
FOLDERS = [
    "../city_data",
    "../hotel_data"
]

def upload_csv_folders_to_s3(folders, bucket_name, region, aws_access_key_id, aws_secret_access_key):
    """
    Upload tous les fichiers CSV de plusieurs dossiers vers S3 en gardant la structure.
    """
    #Initialisation
    s3_client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    for folder_path in folders:
        folder = Path(folder_path)
        csv_files = folder.glob('*.csv')

        for csv_file in csv_files:
            relative_path = csv_file.relative_to(folder.parent)
            s3_key = f"{relative_path}".replace("\\", "/")
            s3_client.upload_file(str(csv_file), bucket_name, s3_key)
def load_s3_to_mysql(bucket_name, db_host, db_user, db_password, db_name, region, aws_access_key_id, aws_secret_access_key):
    """
    Charge les fichiers d'un S3 vers une base de donnée MYSQL.
    """
    #Connexion S3
    s3_client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    #Connexion MYSQL
    engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}')

    #Lister tous les fichiers csv dans le bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    for obj in response.get('Contents', []):
        #Télécharger le csv depuis S3
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
        csv_content = csv_obj['Body'].read().decode('utf-8')

        #Créer un nom de table à partir du nom de fichier
        table_name = Path(obj['Key']).stem

        #Lire avec pandas
        df = pd.read_csv(StringIO(csv_content))

        #Insérer dans MYSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)


upload_csv_folders_to_s3(
    FOLDERS,
    BUCKET_NAME,
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
)

load_s3_to_mysql(
    BUCKET_NAME,
    DB_HOST,
    DB_USER,
    DB_PASSWORD,
    DB_NAME,
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
)

print("Chargement terminé.")