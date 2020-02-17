from sys import argv

import pandas as pd
from pandas import DataFrame

from helper import writeCsvFile, getSongMetadata


def findMissingSongs(dataset: DataFrame, merged: DataFrame) -> list:
    list = []
    for cu_row in merged.itertuples():
        if not cu_row.Index in dataset.index:
            list.append(cu_row.Index)
    return list


def downloadSongsMetadata(ids: list, token: str):
    list = []
    for id in ids:
        print(".", end='')
        response = getSongMetadata(id, token)
        metadata = response.json()
        list.append(metadata)
    print("")
    return list


def completeSongsMetadata(song_list: list, merged: DataFrame):
    for songDict in song_list:
        row = merged.loc[songDict["id"]]
        songDict["artist_name"] = row["Artist"]
        songDict["track_name"] = row["TrackName"]
        songDict["track_id"] = songDict.pop("id", None)
        songDict.pop("type", None)
        songDict.pop("uri", None)
        songDict.pop("track_href", None)
        songDict.pop("analysis_url", None)
        songDict["genre"] = None
        songDict["Popularity"] = None
    pass


def addSongsToMetadata(songs: list, df: DataFrame):
    new_dataframe = df
    for song in songs:
        df.loc[song.pop("track_id")] = song
        print(new_dataframe.tail())
    return new_dataframe


originalDatasetPath, mergedDatasetPath, authToken = argv[1], argv[2], argv[3]
print("Starting...")
dataset = pd.read_csv(originalDatasetPath, encoding="utf-8")
dataset = dataset.set_index(["track_id"], verify_integrity=True)
print(dataset.columns)
merged = pd.read_csv(mergedDatasetPath, encoding="utf-8", index_col=[0])
print("Csv read")
ids_list = findMissingSongs(dataset, merged)
print("There are {0} missing songs".format(len(ids_list)))
print("Downloading songs from spotify")
song_list = downloadSongsMetadata(ids_list, authToken)
print("Songs downloaded, completing their data")
complete_songs = completeSongsMetadata(song_list, merged)
print("Merging datasets")
new_dataset = addSongsToMetadata(song_list, dataset)
writeCsvFile("complete_dataset.csv", new_dataset, True)
print("Done :)")
