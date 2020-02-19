from sys import argv

import pandas as pd
from pandas import DataFrame

from helper import writeCsvFile, getSongMetadata, createFolderIfItDoenstExist


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
    return new_dataframe


def updateMergedWithSpotifyData(merged: DataFrame, authToken: str) -> DataFrame:
    ids_list = merged.index
    print("downloading songs metadata for {0} songs".format( len(ids_list) ))
    metadatas = downloadSongsMetadata(ids_list, authToken)
    metadataDf = pd.DataFrame.from_dict(metadatas)
    metadataDf.drop(["type", "analysis_url", "uri", "track_href"], axis=1, inplace=True)
    metadataDf.set_index(["id"], verify_integrity=True, inplace=True)
    concat = pd.concat([metadataDf, merged], axis=1)
    return concat

def completeMergedDataset(mergedDatasetPath:str, authToken:str):
    print("Starting...")
    merged = pd.read_csv(mergedDatasetPath, encoding="utf-8", index_col=[0])
    print("Csv read")
    tops_metadata = updateMergedWithSpotifyData(merged, authToken)
    createFolderIfItDoenstExist("dataset_completer_out")
    writeCsvFile("dataset_completer_out/tops_with_metadata.csv", tops_metadata, True)
    print("Done :)")

if __name__ == '__main__':
    originalDatasetPath, mergedDatasetPath, authToken = argv[1], argv[2], argv[3]
    completeMergedDataset(mergedDatasetPath, authToken)
    """
    ids_list = findMissingSongs(dataset, merged)
    print("There are {0} missing songs".format(len(ids_list)))
    print("Downloading songs from spotify")
    song_list = downloadSongsMetadata(ids_list, authToken)
    print("Songs downloaded, completing their data")
    complete_songs = completeSongsMetadata(song_list, merged)
    print("Merging datasets")
    new_dataset = addSongsToMetadata(song_list, dataset)
    writeCsvFile("dataset_completer_out/complete_dataset.csv", new_dataset, True)
    print("Done :)")
    """
