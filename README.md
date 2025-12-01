# BikeOPS
projet fil rouge formation Consultant Data Engineer

Nettoyage des données de disponibilité des vélos

La fonction clean_nb_bikes est utilisée pour nettoyer deux colonnes spécifiques : bikes_available (vélos disponibles) et slots_free (emplacements libres). Elle est appliquée successivement sur ces deux colonnes afin d’assurer la cohérence des valeurs par rapport à la capacité totale de la station.

Important : L’ordre d’application de cette fonction est crucial. La deuxième colonne nettoyée est considérée comme prioritaire, et la première colonne est ajustée en fonction des valeurs corrigées de la deuxième. Par conséquent, cette approche implique que la priorité de correction est fixée par l’ordre dans lequel les colonnes sont traitées.

Cette stratégie a été choisie pour permettre une correction automatique tout en conservant une colonne comme référence prioritaire. Toutefois, dans un contexte métier réel, il est recommandé de définir explicitement quelle colonne doit être prioritaire ou, le cas échéant, d’exclure les données incohérentes pour garantir la qualité des analyses.
