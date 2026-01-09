"""
ETL Pipeline : Extract from S3 → Transform → Load to RDS
"""

import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# Charger les variables d'environnement
load_dotenv()

# Configuration AWS
AWS_REGION = 'eu-west-3'
BUCKET_NAME = 'projey-kayak'
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Configuration RDS
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

print("🚀 Démarrage du pipeline ETL...")
print(f"📍 Connexion à RDS : {DB_USER}@{DB_HOST}/{DB_NAME}")

# Test de connexion à RDS
try:
    engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}')
    connection = engine.connect()
    print("✅ Connexion RDS réussie !")
    connection.close()
except Exception as e:
    print(f"❌ Erreur de connexion RDS : {e}")
    exit(1)

print("\n🎉 Configuration OK !")

# Initialiser le client S3
s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

print("\n📥 EXTRACT : Chargement des données depuis S3...")

# Charger top_5_cities.csv
response = s3_client.get_object(
    Bucket=BUCKET_NAME,
    Key='processed/weather/top_5_cities.csv'
)
df_cities = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"✅ df_cities chargé : {len(df_cities)} villes")

# Charger booking_hotels_raw.csv
response = s3_client.get_object(
    Bucket=BUCKET_NAME,
    Key='raw/hotels/booking_hotels_raw.csv'
)
df_hotels = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"✅ df_hotels chargé : {len(df_hotels)} hôtels")

print("\n🔄 TRANSFORM : Merge des données...")

# Merger les hôtels avec les infos des villes
df_hotels_enriched = df_hotels.merge(
    df_cities[['city', 'longitude', 'latitude', 'temp_day_mean', 'clouds_mean', 'pop_mean', 'global_score']],
    on='city',
    how='left'
)

print(f"✅ df_hotels_enriched créé : {len(df_hotels_enriched)} hôtels avec données météo")
print(df_hotels_enriched.head())

print("\n📤 LOAD : Chargement dans RDS...")

# Connexion à RDS avec SQLAlchemy
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}')

# Charger la table cities
df_cities.to_sql('cities', engine, if_exists='replace', index=False)
print(f"✅ Table 'cities' créée : {len(df_cities)} lignes")

# Charger la table hotels
df_hotels_enriched.to_sql('hotels', engine, if_exists='replace', index=False)
print(f"✅ Table 'hotels' créée : {len(df_hotels_enriched)} lignes")

print("\n🎉 Pipeline ETL terminé avec succès !")
print(f"📊 Base de données RDS : {DB_NAME}")
print(f"📍 Tables créées : cities ({len(df_cities)} lignes), hotels ({len(df_hotels_enriched)} lignes)")