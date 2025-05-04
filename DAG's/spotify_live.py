from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd


#  Definfing Default Arguments

DATASET_NAME = 'Top_songs'
TABLE_NAME = "Live_Table"
BUCKET_NAME = 'live_spotify_data'
object_name = 'live_spotify_tracks.json'


def extract_spotify_data(**kwargs):
    client_id = Variable.get("CLIENT_ID")
    client_secret = Variable.get("CLIENT_SECRET")
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    ))

    playlist_id = "6IoAdfkxFwfTY4Ug7TVRMY"
    
    track_data = []
    offset = 0
    limit = 100  

    while True:
        response = sp.playlist_items(playlist_id, offset=offset, limit=limit)
        items = response['items']
        if not items:
            break

        for item in items:
            track = item['track']
            artist_uris = ', '.join([artist['uri'] for artist in track['artists']])
            artist_ids = [artist['id'] for artist in track['artists']]
            artist_names = ', '.join([artist['name'] for artist in track['artists']])
            genres = []
            for artist_id in artist_ids:
                artist_detail = sp.artist(artist_id)
                genres.extend(artist_detail.get('genres', []))

            artist_genres = ', '.join(list(set(genres)))

            album_id  = track['album']['id']
            album = sp.album(album_id)

            track_data.append({
                'track_uri': track['uri'],
                'track_name': track['name'],
                'artist_uri': artist_uris,   
                'artist_names': artist_names,
                'album_uri': track['album']['uri'],
                'album_name' :track['album']['name'],
                'release_date': track['album']['release_date'],
                'disk_number': track['disc_number'],
                'track_number': track['track_number'],
                'duration_ms': track['duration_ms'],
                'explicit': track['explicit'],
                'popularity': track['popularity'],
                'isrc': track['external_ids']['isrc'],
                'added_by' : item['added_by']['id'] if item['added_by'] else None,
                'added_at' : item['added_at'],
                'artist_genres': artist_genres, 
                'label' : album['label'],
                'copyrights' : album['copyrights']
            })

        offset += limit  

    
    kwargs['ti'].xcom_push(key='track_data', value=track_data)    

def preprocess_spotify_data(**kwargs):
    track_data = kwargs['ti'].xcom_pull(task_ids='extract_spotify_data', key='track_data')
    df_tracks = pd.DataFrame(track_data)

    # Preprocessing
  
    def copyrights(row):
        if isinstance(row, list):
            return ', '.join(
                f"{c.get('type', '')} {c.get('text', '')}" for c in row
            )
        return None

    df_tracks['copyrights'] = df_tracks['copyrights'].apply(copyrights)    
    df_tracks['duration_min'] = df_tracks['duration_ms'] / 60000
    df_tracks.drop(columns={'duration_ms'}, inplace=True)
    df_tracks.drop_duplicates(inplace=True)

    processed_data = df_tracks.to_dict(orient='records')

    # Push processed data to XCom
    kwargs['ti'].xcom_push(key='processed_track_data', value=processed_data)

def upload_preprocessed_to_gcs(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    # Get data after preprocessing
    cleaned_data = kwargs['ti'].xcom_pull(task_ids='preprocess_spotify_data', key='processed_track_data')
    # Converting the data dictonory to JSON where each dictionary becomes a single JSON object on a new line.
    json_data = '\n'.join([json.dumps(record) for record in cleaned_data])


    # Upload to GCS
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=object_name, 
        data=json_data,                               
        mime_type='application/json'
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('spotify_etl_pipeline',
         default_args=default_args,
         schedule='@hourly',
         catchup=False) as dag:

    extraction = PythonOperator(
        task_id='extract_spotify_data',
        python_callable=extract_spotify_data,
    )

    preprocessing = PythonOperator(
        task_id='preprocess_spotify_data',
        python_callable=preprocess_spotify_data,
    )

    upload = PythonOperator(
        task_id='upload_preprocessed_to_gcs',
        python_callable=upload_preprocessed_to_gcs,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[object_name],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format='NEWLINE_DELIMITED_JSON',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE'
    )

    extraction >> preprocessing >> upload >> load_to_bq
