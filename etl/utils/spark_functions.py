# region IMPORTS
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from pathlib import Path
# endregion


# region READ CSV
def read_csv_spark(
    spark: SparkSession, path: str, col: list = None, *, delimiter: str = ";"
) -> DataFrame:
    """
    Lit un fichier avec Spark et retourne un DataFrame.

    Args:
        spark (SparkSession): La session Spark.
        path (str): Le chemin du fichier à lire.
        options (dict): Un dictionnaire d'options pour la lecture du fichier.

    Returns:
        DataFrame: Le DataFrame résultant de la lecture du fichier.
    """
    if col is not None:
        df = (
            spark.read.option("header", True)
            .option("sep", delimiter)
            .option("mode", "DROPMALFORMED")
            .csv(path)
            .select(*col)
        )
        return df
    df = (
        spark.read.option("header", True)
        .option("sep", delimiter)
        .option("mode", "DROPMALFORMED")
        .csv(path)
    )
    return df


# endregion


# region APPLY TRANSFORMATIONS
def apply_transformations(
    df,
    transformations: list[dict[str, any]],
    *,
    score: bool = False,
    drop: list[str] = None,
) -> DataFrame:
    """
    Applique une série de transformations à un DataFrame Spark.

    Args:
        df (DataFrame): Le DataFrame à transformer.
        transformations (list of dict): Liste de transformations à appliquer. Chaque dictionnaire doit contenir :
            - 'col' (str): Le nom de la colonne à transformer.
            - 'func' (callable): La fonction de transformation à appliquer.
            - 'args' (list, optional): Arguments positionnels supplémentaires pour la fonction.
            - 'kwargs' (dict, optional): Arguments nommés supplémentaires pour la fonction.
        score (bool, optional): Si True, ajoute une colonne 'score' comptant les valeurs nulles ou vides. Par défaut False.
        drop (list of str, optional): Liste de colonnes à supprimer après les transformations. Par défaut None.

    Returns:
        DataFrame: Le DataFrame transformé.
    """
    # obtenir la liste des colonnes initiales
    cols = df.columns

    # Initialiser les colonnes accumulateurs avec False
    df = df.withColumn("lignes_corrigees", F.lit(False)).withColumn(
        "valeurs_invalides", F.lit(False)
    )

    for t in transformations:
        col_name = t["col"]
        func = t["func"]
        args = t.get("args", [])
        kwargs = t.get("kwargs", {})

        # Application des transformations (fonctions UDF retournant un STRUCT)
        df = df.withColumn(col_name, func(*([F.col(col_name)] + args), **kwargs))

        # Mise à jour des colonnes accumulateurs
        df = df.withColumn(
            "lignes_corrigees",
            F.col(f"{col_name}.ligne_corrigee") | F.col("lignes_corrigees"),
        ).withColumn(
            "valeurs_invalides",
            F.col(f"{col_name}.ligne_invalide") | F.col("valeurs_invalides"),
        )

        # Remplace la colonne par le champ nettoyé
        df = df.withColumn(col_name, F.col(f"{col_name}.value"))

    # compte le nombre de colonnes contenant des valeurs null ou vides pour chaque ligne
    if score:
        # pour chaque colonne, on crée une condition : 1 si null ou vide, sinon 0
        score_expr = sum(
            F.when(F.col(c)["value"].isNull() | (F.col(c)["value"] == ""), 1).otherwise(
                0
            )
            if df.schema[c].dataType.typeName()
            == "struct"  # Vérifie si c'est un STRUCT
            else F.when(F.col(c).isNull() | (F.col(c) == ""), 1).otherwise(0)
            for c in cols
        )
        df = df.withColumn("score", score_expr)

    # supprime les colonnes spécifiées
    if drop:
        df = df.drop(*drop)

    return df


# endregion

# region REPORT CLEANUP


def process_report_and_cleanup(df: DataFrame) -> dict[str, int]:
    """
    Compte les colonnes `lignes_corrigees`, `valeurs_invalides`, et `lignes_supprimees`,
    génère un rapport, et supprime ces colonnes du DataFrame.

    Args:
        df (DataFrame): Le DataFrame Spark à traiter.

    Returns:
        dict(str, int): Un dictionnaire contenant le rapport.
            - 'total_lignes_corrigees': Nombre total de lignes corrigées.
            - 'total_valeurs_invalides': Nombre total de valeurs invalides.
    """
    # Compter les valeurs True dans les colonnes spécifiques
    rapport = (
        df.select(
            F.sum(F.col("lignes_corrigees").cast("int")).alias(
                "total_lignes_corrigees"
            ),
            F.sum(F.col("valeurs_invalides").cast("int")).alias(
                "total_valeurs_invalides"
            ),
        )
        .collect()[0]
        .asDict()
    )

    # Supprimer les colonnes inutiles sans retourner le DataFrame modifié
    df = df.drop("lignes_corrigees", "valeurs_invalides", "lignes_supprimees")

    return df, rapport


# endregion

# region DROP DUPLICATES


def dedupe_by_partition_order(
    df: DataFrame, partition_cols: list[str], order_col: str
) -> DataFrame:
    """
    Supprime les doublons dans un DataFrame Spark en gardant la ligne avec la meilleure valeur
    dans une colonne spécifiée.

    Args:
        df (DataFrame): Le DataFrame Spark à traiter.
        partition_cols (list of str): Les colonnes pour partitionner les données.
        order_col (str): La colonne utilisée pour déterminer la "meilleure" ligne.

    Returns:
        DataFrame: Le DataFrame sans doublons.
    """

    # group by station_id et timestamp, order by score (croissant)
    dup_wind = Window.partitionBy(*partition_cols).orderBy(order_col)

    # on numérote les lignes par groupe défini par partition_cols (par order_col croissant)
    df = df.withColumn("row_number", F.row_number().over(dup_wind))

    # garder la ligne avec le score le plus bas
    df = df.filter(F.col("row_number") == 1).drop("row_number")

    return df


# endregion

# region CREATE SILVER DF


def create_silver_df(
    df: DataFrame,
    transformations: list[dict],
    *,
    score: bool = False,
    duplicates_drop: list = None,
    partition_col: str = None,
    drop_cols: list[str] = None,
) -> tuple[DataFrame, dict]:
    """
    Création du DataFrame silver avec nettoyage et rapport qualité.
    /!\ Nécessite une session Spark existante. /!\ 

    Args:
        df (DataFrame): Le DataFrame brut à nettoyer.
        transformations (list[dict]): Liste des transformations à appliquer.
            dictionnaire avec les clés :
            - 'col' (str): Le nom de la colonne à transformer.
            - 'func' (callable): La fonction de transformation à appliquer.
            - 'args' (list, optional): Arguments positionnels supplémentaires pour la fonction.
        partition_col (str, optional): Colonne pour partitionner les données. Defaults to None.

    Returns:
        tuple: Un tuple contenant :
        - df_clean (DataFrame): 
            DataFrame Spark nettoyé.
        - rapport_value (dict): 
            Rapport de qualité des données, avec les clés suivantes :
            - 'total_lignes_brutes' (int): Nombre de lignes brutes.
            - 'total_lignes_corrigees' (int): Nombre total de lignes corrigées.
            - 'total_valeurs_invalides' (int): Nombre total de valeurs invalides.
            - 'total_lignes_supprimees' (int): Nombre de lignes supprimées.

        """
    # nombre de lignes brutes => pour rapport qualité
    lignes_brutes = df.count()

    # suite au transformations udf, timestamp est un struct avec value, ligne_corrigee, ligne_invalide
    df_clean = apply_transformations(df, transformations, score=score)

    if partition_col:
        df_clean = df_clean.withColumn(
            "date_partition", F.to_date(F.col(partition_col))
        )

    # génération du rapport de qualité et nettoyage des colonnes accumulateurs
    df_clean, rapport_value = process_report_and_cleanup(df_clean)

    # suppression des doublons si demandé
    if duplicates_drop and score:
        df_clean = dedupe_by_partition_order(
            df_clean, partition_cols=[*duplicates_drop], order_col="score"
        )

    # suppression des lignes où toutes les valeurs sont nulles
    df_clean = df_clean.dropna(how="all")
    lignes_supprimees = lignes_brutes - df_clean.count()

    # mise à jour du rapport qualité
    rapport_value["total_lignes_supprimees"] = lignes_supprimees
    rapport_value["total_lignes_brutes"] = lignes_brutes

    if drop_cols:
        df_clean = df_clean.drop(*drop_cols)

    return df_clean, rapport_value


# endregion

# region QUALITY REPORT


def quality_rapport(
    rapport_value: dict[str, int],
    rapport_file_name: str,
    base_path: str | Path = "/app/data/data_clean/rapport_qualite",
) -> None:
    """
    Génère un rapport de qualité des données et l'écrit dans un fichier texte.

    Args:
        rapport_value (dict): Dictionnaire contenant les statistiques de qualité des données.
        rapport_file_name (str): Nom du fichier dans lequel écrire le rapport.
        base_path (str | Path, optional): Chemin de base pour enregistrer le rapport. Par défaut "/app/data/data_clean/rapport_qualite".

    Returns:
        None (écrit le rapport dans un fichier texte dans le répertoire spécifié).
    """
    rapport = [
        f"Rapport qualité - {rapport_file_name}",
        "-------------------------------------",
        f"- Lignes brutes : {rapport_value['total_lignes_brutes']}",
        f"- Lignes corrigées : {rapport_value['total_lignes_corrigees']}",
        f"- Lignes invalidées (remplacées par None) : {rapport_value['total_valeurs_invalides']}",
        f"- Lignes supprimées : {rapport_value['total_lignes_supprimees']}",
    ]

    # Convertir en Path si ce n'est pas déjà le cas
    base_path = Path(base_path)

    # Création du répertoire
    base_path.mkdir(parents=True, exist_ok=True)

    # Écriture du rapport qualité dans un fichier texte
    with open(base_path / f"{rapport_file_name}_rapport.txt", "w") as f:
        f.write("\n".join(rapport))


# endregion
