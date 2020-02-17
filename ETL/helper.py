import io
import os

import pandas as pd
from pandas import DataFrame
from requests import get


def downloadFile(url: str):
    response = get(url)
    if response.status_code == 404:
        return None
    return response.content


def writeFile(name: str, content):
    with open(name, "wb") as file:
        file.write(content)


def writeCsvFile(name: str, dataframe: DataFrame, indexB = False):
    dataframe.to_csv(name, encoding='utf-8', index=indexB)


def folderExist(folderpath: str) -> bool:
    return os.path.isdir(folderpath)


def createFolder(folderpath: str):
    os.makedirs(folderpath)


def createFolderIfItDoenstExist(folderpath: str):
    if not folderExist(folderpath):
        createFolder(folderpath)


def createDataframeFromCsv(bytes) -> DataFrame:
    return pd.read_csv(io.StringIO(bytes.decode('utf-8')), skiprows=1)

def getSongMetadata(songId : str, authToken: str):
    """https://developer.spotify.com/console/get-audio-features-track/?id="""
    url = "https://api.spotify.com/v1/audio-features/{0}".format(songId)
    headers = {"Authorization": "Bearer {0}".format(authToken),
               "Accept": "application/json",
               "Content-Type": "application/json"}
    response = get(url, headers= headers)
    if not response.status_code == 200:
        raise Exception("Cannot download song id: {0} with token: {1}. Error is: {2}".format(songId, authToken, response.content))
    return response
