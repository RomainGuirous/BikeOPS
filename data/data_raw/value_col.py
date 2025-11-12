import pandas as pd

# Remplace le nom du fichier par le tien si besoin
df = pd.read_csv('weather_raw.csv', sep=";")

# Affiche les valeurs uniques de la colonne 'status'
print(df['weather_condition'].unique())