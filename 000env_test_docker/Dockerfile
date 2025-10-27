# Image officielle Spark + Python
FROM godatadriven/pyspark 

# Définir le répertoire de travail dans le container
WORKDIR /app

# Copier ton projet local dans le container (optionnel si tu veux monter un volume)
# COPY . .

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

# docker run -it --rm -v "${PWD}:/app" --name env_test_bike --entrypoint /bin/bash env_test_bikeops
# -v "${PWD}:/app" 