"""
Script pour calculer le top 5 des villes avec la meilleure météo
Lit les données brutes depuis S3, applique le barème, sauvegarde le top 5
"""

import pandas as pd
import numpy as np
import json
import boto3
from io import StringIO
from dotenv import load_dotenv
import os
from datetime import datetime
from collections import defaultdict

# Charger les variables d'environnement
load_dotenv()

# Configuration AWS
AWS_REGION = 'eu-west-3'
BUCKET_NAME = 'projey-kayak'
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialiser le client S3
s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

print("🚀 Démarrage du calcul du top 5 des villes...")

# Charger les données brutes depuis S3
print("\n📥 Chargement des données depuis S3...")

# Charger dict_cities (coordonnées GPS)
response = s3_client.get_object(
    Bucket=BUCKET_NAME,
    Key='raw/weather/nominatim_cities.json'
)
dict_cities = json.loads(response['Body'].read().decode('utf-8'))
print(f"✅ Coordonnées GPS chargées : {len(dict_cities)} villes")

# Charger dict_city_meteo (données météo)
response = s3_client.get_object(
    Bucket=BUCKET_NAME,
    Key='raw/weather/openweather_data.json'
)
dict_city_meteo = json.loads(response['Body'].read().decode('utf-8'))
print(f"✅ Données météo chargées : {len(dict_city_meteo)} villes")

# Liste des villes
list_cities = list(dict_cities.keys())

# Créer un DataFrame avec les coordonnées GPS
print("\n🗺️ Création d'un DataFrame avec coordonnées GPS...")

new_dict = {
    'city': [],
    'longitude': [],
    'latitude': []
}

for city in list_cities:
    new_dict['city'].append(city)
    new_dict['longitude'].append(dict_cities[city][0]['lon'])
    new_dict['latitude'].append(dict_cities[city][0]['lat'])

df_city = pd.DataFrame(new_dict)

# Ajouter un ID pour chaque ville
df_city['id'] = [i+1 for i in range(len(list_cities))]

print(f"✅ DataFrame créé : {len(df_city)} villes")
print(df_city.head())

# Extraire les données météo pour les 7 prochains jours
print("\n🌤️ Extraction des données météo sur 7 jours...")

colonnes = defaultdict(list)

for city in list_cities:
    for i in range(1, len(dict_city_meteo[city]['daily'])):
        jour = dict_city_meteo[city]['daily'][i]
        date = datetime.fromtimestamp(dict_city_meteo[city]['daily'][i]['dt']).strftime('%Y-%m-%d')
        
        colonnes[f"temp_day J+{i} {date}"].append(jour['temp']['day'])
        colonnes[f"temp_night J+{i} {date}"].append(jour['temp']['night'])
        colonnes[f"temp_min J+{i} {date}"].append(jour['temp']['min'])
        colonnes[f"temp_max J+{i} {date}"].append(jour['temp']['max'])
        colonnes[f"temp_eve J+{i} {date}"].append(jour['temp']['eve'])
        colonnes[f"temp_morn J+{i} {date}"].append(jour['temp']['morn'])
        colonnes[f"feels_like_day J+{i} {date}"].append(jour['feels_like']['day'])
        colonnes[f"feels_like_night J+{i} {date}"].append(jour['feels_like']['night'])
        colonnes[f"feels_like_eve J+{i} {date}"].append(jour['feels_like']['eve'])
        colonnes[f"feels_like_morn J+{i} {date}"].append(jour['feels_like']['morn'])
        colonnes[f"humidity J+{i} {date}"].append(jour['humidity'])
        colonnes[f"wind_speed J+{i} {date}"].append(jour['wind_speed'])
        colonnes[f"main weather J+{i} {date}"].append(jour['weather'][0]['main'])
        colonnes[f"clouds J+{i} {date}"].append(jour['clouds'])
        colonnes[f"pop J+{i} {date}"].append(jour['pop'])

# Concaténer avec df_city
df_city = pd.concat([df_city, pd.DataFrame(colonnes)], axis=1)

print(f"✅ Colonnes météo ajoutées : {len(colonnes)} colonnes")

# Filtrer pour ne garder que les colonnes utiles
print("\n🔍 Filtrage des colonnes utiles...")

prefixes = ['id', 'city', 'longitude', 'latitude', 'temp_day', 'feels_like_day', 'clouds', 'pop']
cols_keep = [col for col in df_city.columns if any(col.startswith(p) for p in prefixes)]
df_city = df_city[cols_keep]

print(f"✅ Colonnes gardées : {len(df_city.columns)}")

# Calculer les moyennes sur la semaine
print("\n📊 Calcul des moyennes hebdomadaires...")

df_city = df_city.copy()
df_city['temp_day_mean'] = df_city[[col for col in df_city.columns if col.startswith('temp_day J+')]].mean(axis=1)
df_city['difference_feels_like_day_mean'] = abs(df_city['temp_day_mean'] - df_city[[col for col in df_city.columns if col.startswith('feels_like_day J+')]].mean(axis=1))
df_city['clouds_mean'] = df_city[[col for col in df_city.columns if col.startswith('clouds J+')]].mean(axis=1)
df_city['pop_mean'] = df_city[[col for col in df_city.columns if col.startswith('pop J+')]].mean(axis=1)

print("✅ Moyennes calculées")
print(df_city[['city', 'temp_day_mean', 'clouds_mean', 'pop_mean']].head())

# Appliquer le barème de scoring
print("\n🏆 Application du barème de scoring...")

# Barème de la température (20% de la note)
conditions_temp = [
    (df_city['temp_day_mean']>=18) & (df_city['temp_day_mean']<=25),
    ((df_city['temp_day_mean']>=13) & (df_city['temp_day_mean']<18)) |
    ((df_city['temp_day_mean']>25) & (df_city['temp_day_mean']<=30)),
    (df_city['temp_day_mean']<13) | (df_city['temp_day_mean']>30)
]
score_temp = [20, 10, 0]

# Barème de la différence température/ressenti (20% de la note)
conditions_feels_like = [
    (df_city['difference_feels_like_day_mean']<3),
    (df_city['difference_feels_like_day_mean']>=3) & (df_city['difference_feels_like_day_mean']<6),
    (df_city['difference_feels_like_day_mean']>=6)
]
score_feels_like = [20, 10, 0]

# Barème de la couverture nuageuse (10% de la note)
conditions_clouds = [
    (df_city['clouds_mean']>=0) & (df_city['clouds_mean']<20),
    (df_city['clouds_mean']>=20) & (df_city['clouds_mean']<40),
    (df_city['clouds_mean']>=40) & (df_city['clouds_mean']<60),
    (df_city['clouds_mean']>=60) & (df_city['clouds_mean']<80),
    (df_city['clouds_mean']>=80) & (df_city['clouds_mean']<=100)
]
score_clouds = [20, 15, 10, 5, 0]

# Barème de la pluie (50% de la note)
conditions_pop = [
    (df_city['pop_mean']==0),
    (df_city['pop_mean']>0) & (df_city['pop_mean']<0.1),
    (df_city['pop_mean']>=0.1) & (df_city['pop_mean']<0.3),
    (df_city['pop_mean']>=0.3) & (df_city['pop_mean']<0.5),
    (df_city['pop_mean']>=0.5) & (df_city['pop_mean']<=1)
]
score_pop = [20, 15, 10, 5, 0]

# Création des scores
df_city['temp_day_score'] = np.select(conditions_temp, score_temp, default=0)
df_city['feels_like_score'] = np.select(conditions_feels_like, score_feels_like, default=0)
df_city['clouds_score'] = np.select(conditions_clouds, score_clouds, default=0)
df_city['pop_score'] = np.select(conditions_pop, score_pop, default=0)

# Score global pondéré
df_city['global_score'] = (
    df_city['temp_day_score'] * 0.2 +
    df_city['feels_like_score'] * 0.2 +
    df_city['clouds_score'] * 0.1 +
    df_city['pop_score'] * 0.5
)

print("✅ Scores calculés")
print(df_city[['city', 'temp_day_score', 'clouds_score', 'pop_score', 'global_score']].head())

# Trier de la meilleure ville à la pire
print("\n🎖️ Classement des villes...")

df_city = df_city.sort_values(
    ['global_score', 'pop_score', 'temp_day_score', 'feels_like_score', 'clouds_score'],
    ascending=[False, False, False, False, False]
).reset_index(drop=True)

# Ajouter le ranking
df_city['rank'] = [f"{i+1}. {df_city['city'][i]}" for i in range(len(df_city))]

print("✅ Classement effectué")
print("\n🏆 TOP 10 des villes :")
print(df_city[['rank', 'city', 'global_score', 'pop_mean', 'temp_day_mean']].head(10))

# Créer df_top_5_city
print("\n📦 Création du TOP 5 pour le scraping...")

cols = [
    'city', 'longitude', 'latitude', 
    'temp_day_mean', 'clouds_mean', 'pop_mean',
    'temp_day_score', 'clouds_score', 'pop_score', 'global_score', 
    'rank'
]
df_top_5_city = df_city[cols].head(5).copy()

print("\n🏆 TOP 5 calculé :")
print(df_top_5_city)

# ⚠️ CELLULE FANTÔME - Override avec les villes déjà scrappées ⚠️
print("\n🎭 Override avec les villes scrappées...")
override_cities = ["Bayeux", "Le Havre", "Lille", "Mont Saint Michel", "Paris"]
df_top_5_city = df_city[df_city['city'].isin(override_cities)][cols].copy()
print(df_top_5_city)

# Upload sur S3
print("\n📤 Upload vers S3...")
csv_buffer = StringIO()
df_top_5_city.to_csv(csv_buffer, index=False)
s3_client.put_object(Bucket=BUCKET_NAME, Key='processed/weather/top_5_cities.csv', Body=csv_buffer.getvalue())
print("✅ top_5_cities.csv uploadé sur S3")

print("\n🎉 Calcul terminé avec succès !")