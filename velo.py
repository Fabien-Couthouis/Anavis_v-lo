# %%
import os
import sys
import shutil
import pandas as pd
from dateutil import parser
from zipfile import ZipFile

from joblib.memory import Memory
# %%lire les données brutes directement depuis le fichier zip
with ZipFile('brut.zip') as myzip:
    print(myzip.namelist())
    with myzip.open('brut/bicincitta_parma_summary.csv') as summary:
        summary_df = pd.read_csv(summary, sep=";", encoding="utf-8")
    with myzip.open('brut/status_bicincitta_parma.csv') as status:
        status_df = pd.read_csv(status, sep=";", encoding="utf-8",
                                names=["date", "Station", "Status", "Nombre de vélos disponibles", "Nombre d'emplacements disponibles"])
    with myzip.open('brut/weather_bicincitta_parma.csv') as weather:
        weather_df = pd.read_csv(weather, sep=";", encoding="utf-8",
                                 names=["Timestamp", "Status", "Clouds", "Humidity", "Pressure", "Rain", "WindGust", "WindVarEnd", "WindVarBeg", "WindDeg", "WindSpeed", "Snow", "TemperatureMax", "TemperatureMin", "TemperatureTemp"])

# %%
summary_df.head()
# %%
status_df.head()
# %%
weather_df.sample(30)

# %%
summary_df_sample = summary_df[0:1000]
status_df_sample = status_df[0:1000]

# %% supprimer les données abérentes


def parse_date(str_date):
    try:
        date = parser.parse(str_date)
    except Exception:
        date = None
    return date


# dates
status_df_sample["parsed_date"] = status_df_sample["date"].apply(
    lambda x: parse_date(x))

# status
status_df_sample = status_df_sample[status_df_sample["Status"] != 0]

# %%normaliser le nom des stations


def get_station_names(df):
    stations_with_nb = df["station"].unique()
    station_without_nb = []
    for station in stations_with_nb:
        station_without_nb.append(station[4:])
    return stations_with_nb, station_without_nb


def normalize_stations(name, stations_with_nb, station_without_nb):
    for idx, station in enumerate(station_without_nb):
        if station in name:
            name = stations_with_nb[idx]
    return name


stations_with_nb, station_without_nb = get_station_names(summary_df_sample)
status_df_sample["Station"] = status_df_sample["Station"].apply(
    (lambda x: normalize_stations(x, stations_with_nb, station_without_nb)))
status_df_sample.head()

# %%ré-échantilloner les données pour avoir un enregistrement toutes les 10 minutes

status_df_sample = status_df_sample.set_index(
    'parsed_date')
status_df_sample.resample('10min')
status_df_sample.head()
# %%

status_df_sample = status_df_sample.drop(columns=['Status'])

# %%
status_df_sample.head()

# %% Simplify weather
weather_df_sample = weather_df[0:1000]

weather_df_sample = weather_df_sample.drop(columns=[
    'WindGust', 'WindVarEnd', 'WindVarBeg', 'TemperatureMax', 'TemperatureMin'])

# %% Resample weather
weather_df_sample["Timestamp"] = weather_df_sample["Timestamp"].apply(
    lambda x: parse_date(x))
weather_df_sample = weather_df_sample.dropna(subset=['Timestamp'])


weather_df_sample = weather_df_sample.set_index(
    'Timestamp').resample('10min').reset_index()
weather_df_sample.head()


# %%Merge
merge_df = status_df_sample.merge(
    weather_df_sample, left_on='parsed_date', right_on='Timestamp')

# %%
merge_df.head()
merge_df = merge_df.drop(columns=['date'])
# %%
