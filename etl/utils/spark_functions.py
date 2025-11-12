from pyspark.sql import SparkSession, DataFrame, functions as F

def read_csv_spark(spark: SparkSession, path: str, col: list= None) -> DataFrame:
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
            .option("sep", ";")
            .option("mode", "DROPMALFORMED")
            .csv(path)
            .select(*col)
        )
        return df
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("mode", "DROPMALFORMED")
        .csv(path)
    )
    return df


def apply_transformations(df, transformations: list[dict[str, any]], *, score: bool=False, drop: list[str] = None) -> DataFrame:
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
    for t in transformations:
        col_name = t["col"]
        func = t["func"]
        args = t.get("args", [])
        kwargs = t.get("kwargs", {})
        df = df.withColumn(col_name, func(*([col_name] + args), **kwargs))

        # compte le nombre de colonnes contenant des valeurs null ou vides pour chaque ligne
        if score:
            cols = [t["col"] for t in transformations]  # colonnes à inclure dans le score
            # pour chaque colonne, on crée une condition : 1 si null ou vide, sinon 0
            score_expr = sum(
                F.when(F.col(c).isNull() | (F.col(c) == ""), 1).otherwise(0)
                for c in cols
            )
            df = df.withColumn("score", score_expr)
        
        # supprime les colonnes spécifiées
        if drop:
            df = df.drop(*drop)

        return df