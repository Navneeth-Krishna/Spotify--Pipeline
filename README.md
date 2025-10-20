# Spotify Data Pipeline

![Spotify](https://img.shields.io/badge/Spotify-Data_Pipeline-1DB954?style=for-the-badge&logo=spotify&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Google Cloud](https://img.shields.io/badge/GoogleCloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)

A modern data pipeline that extracts, processes, and analyzes Spotify music data using Apache Airflow, PySpark, dbt, and Google Cloud Platform.

##  Overview

This project implements a comprehensive data pipeline that:
- Extracts data from Spotify's API using two approaches:
  - Historical data collection
  - Live data streaming from playlists
- Processes the data using PySpark on Google Cloud Dataproc
- Loads the processed data into Google BigQuery
- Transforms and models the data using dbt for analytics

##  Key Features

- **Dual Data Collection**: Both historical and live data collection from Spotify
- **Automated Pipeline**: Fully automated ETL process using Apache Airflow DAGs
- **Scalable Processing**: PySpark-based data processing on Google Cloud Dataproc
- **Advanced Analytics**: dbt models for:
  - Genre analysis and trends
  - Artist popularity metrics
  - Track age analysis
  - Label activity tracking
- **Cloud-Native**: Leverages Google Cloud Platform for scalable and reliable data processing


## Prerequisites

- Python 3.x
- Google Cloud Platform account
- Spotify Developer account
- Apache Airflow
- dbt

## Configuration

1. Set up Spotify API credentials in Airflow variables:
   ```
   CLIENT_ID = "Your Spotify application client ID"
   CLIENT_SECRET = "Your Spotify application client secret"
   ```

2. Set up Google Cloud credentials:
   - Place your GCP project's service account JSON file in the `gcp_creds` folder as `my-gcp-service-account.json`

3. Install required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

##  Data Models

The project includes several dbt models for analysis:
- `spotify_historic.sql`: Historical track analysis
- `spotify_live.sql`: Live tracking data
- `genre_summary.sql`: Genre-based analytics
- `track_age.sql`: Track age distribution
- `top_artists_by_popularity.sql`: Artist popularity rankings


##  Author

- [Navneeth Krishna Aravind](https://github.com/Navneeth-Krishna)


