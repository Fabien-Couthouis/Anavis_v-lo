# %% Imports and inits
import os
import sys
import pandas as pd
from dateutil import parser
from zipfile import ZipFile
from tqdm import tqdm, tqdm_notebook
tqdm().pandas()

# %%Read data from zip file
zipfile_name = 'brut.zip'
print("Reading ", zipfile_name, "...")
try:
    with ZipFile(zipfile_name) as myzip:
        print("Filenames: ", myzip.namelist())
        with myzip.open('brut/bicincitta_parma_summary.csv') as summary:
            summary_df = pd.read_csv(
                summary, sep=";", encoding="utf-8")
        with myzip.open('brut/status_bicincitta_parma.csv') as status:
            status_df = pd.read_csv(status, sep=";", encoding="utf-8",
                                    names=["date", "Station", "Status", "Nombre de vélos disponibles", "Nombre d'emplacements disponibles"])
        with myzip.open('brut/weather_bicincitta_parma.csv') as weather:
            weather_df = pd.read_csv(weather, sep=";", encoding="utf-8",
                                     names=["Timestamp", "Status", "Clouds", "Humidity", "Pressure", "Rain", "WindGust", "WindVarEnd", "WindVarBeg", "WindDeg", "WindSpeed", "Snow", "TemperatureMax", "TemperatureMin", "TemperatureTemp"])
except Exception as e:
    print("Error: data loading failed. ", e)


# %% Delete aberrant data

# parse dates


def parse_date(str_date):
    try:
        date = parser.parse(str_date)
    except Exception:
        date = None
    return date


print("Parsing status dates ...")
status_df["Timestamp"] = status_df["date"].progress_apply(
    lambda x: parse_date(x))

# select only status != 0
status_df = status_df[status_df["Status"] != 0]

# normalize station names


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


stations_with_nb, station_without_nb = get_station_names(summary_df)
print("Normalizing station names ...")
status_df["Station"] = status_df["Station"].progress_apply(
    (lambda x: normalize_stations(x, stations_with_nb, station_without_nb)))


# %ré-échantilloner les données pour avoir un enregistrement toutes les 10 minutes
print("Resampling status timestamps ...")
status_df = status_df.set_index('Timestamp').groupby(
    'Station')
status_df = status_df.progress_apply(
    lambda x: x.resample("10min").last().reset_index())


status_df.head()


# %%Simplify weather

weather_df = weather_df.drop(columns=[
    'WindGust', 'WindVarEnd', 'WindVarBeg', 'TemperatureMax', 'TemperatureMin', 'Clouds'])

print("Resampling weather ...")
weather_df["Timestamp"] = weather_df["Timestamp"].progress_apply(
    lambda x: parse_date(x))
# Suppression des lignes sans timestamp
weather_df = weather_df.dropna(subset=['Timestamp'])

weather_df = weather_df.set_index('Timestamp')
print("Resampling weather timestamps ...")
weather_df = weather_df.progress_apply(lambda x: x.resample("10min").last())

weather_df.head()


# %%Merge
# Delete useless columns before merging
status_df = status_df.drop(columns=['Status', 'date'])
merged_df = status_df.merge(
    weather_df, left_on='Timestamp', right_on='Timestamp')


# %%stocker les données transformées sous la forme suivante :
# 1 dossier différent par station (soit 24 dossiers) (voir groupby);
# Dans chaque dossier, 1 fichier par sous-série temporelle au format .csv.gz.
# Une sous série temporelle contient tous les exemples consécutif d'une station.
# Le nombre de sous séries dépend du nombre de "trous" dans les données
# (i.e., le nombre de fois où il y a plus de 10 minutes d'écart entre deux
# enregistrements consécutifs).


def save_processed_data(merged_df, data_dir="Data"):
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    for station_name, df in tqdm(merged_df.groupby("Station")):

        # Create directory
        station_dir = os.path.join(data_dir, station_name.upper())
        if not os.path.exists(station_dir):
            os.mkdir(station_dir)

        # end_of_subseries = True if the row is the first element of
        # the next subseries, False otherwise
        df["end_of_subseries"] = df["Timestamp"] - \
            df['Timestamp'].shift() > pd.Timedelta("10min")

        start_idx = 0
        end_idx = 0
        subseries_nb = 1
        for index, (col, row) in enumerate(df.iterrows()):
            if row["end_of_subseries"] == True or index == len(df)-1:
                path = os.path.join(station_dir,
                                    "{}_{}.csv.gz".format(station_name, subseries_nb))
                # drop temp column
                df.drop(columns=["end_of_subseries"])
                # save df from start index to end idx (non included)
                df.iloc[start_idx:end_idx].to_csv(
                    path, compression='gzip', index=False, sep=";", encoding="utf-8")

                subseries_nb += 1
                start_idx = end_idx
            else:
                end_idx += 1


print("Saving data ...")
save_processed_data(merged_df)
