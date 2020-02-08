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


def writeCsvFile(name: str, dataframe: DataFrame):
    dataframe.to_csv(name, encoding='utf-8', index=False)


def folderExist(folderpath: str) -> bool:
    return os.path.isdir(folderpath)


def createFolder(folderpath: str):
    os.makedirs(folderpath)


def createFolderIfItDoenstExist(folderpath: str):
    if not folderExist(folderpath):
        createFolder(folderpath)


def createDataframeFromCsv(bytes) -> DataFrame:
    return pd.read_csv(io.StringIO(bytes.decode('utf-8')), skiprows=1)
