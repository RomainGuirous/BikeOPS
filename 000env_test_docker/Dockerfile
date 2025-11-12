# Image officielle Spark + Python
FROM godatadriven/pyspark 

# Définir le répertoire de travail dans le container
WORKDIR /app

# Installer les dépendances Python nécessaires pour ton projet
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        pandas \
        numpy \
        tensorflow \
        scikit-learn \
        matplotlib \
        seaborn \
        pytest \
        streamlit \
        sqlalchemy \
        pyarrow \
        google-cloud-storage \
        google-cloud-bigquery \
        kafka-python \
        apache-airflow

# Commande par défaut : ouvrir un shell bash interactif
CMD ["/bin/bash"]

# création image:
# docker build -t env_test_bike 000env_test_docker/

# lancer container avec bind-mount (projet) volume:
# avec powershell:
# docker run -it --rm -v "${PWD}:/app" --name cont_test_bike --entrypoint /bin/bash env_test_bike

# avec git-bash:
# MSYS_NO_PATHCONV=1 docker run -it --rm -v "$(pwd):/app" --name cont_test_bike --entrypoint /bin/bash env_test_bike
# ==> Git Bash (MSYS) transforme automatiquement les arguments qui ressemblent à des chemins Unix en chemins Windows.
# L'option MSYS_NO_PATHCONV=1 empêche cette conversion automatique, permettant ainsi de monter correctement les volumes Docker.