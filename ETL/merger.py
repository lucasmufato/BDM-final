import os
from sys import argv

import pandas as pd
from pandas import DataFrame

from helper import folderExist, writeCsvFile


def firstTop10(pos: int):
    if pos <= 10:
        return 1
    return 0


def firstTop25(pos: int):
    if 10 < pos <= 25:
        return 1
    return 0


def firstTop50(pos: int):
    if 25 < pos <= 50:
        return 1
    return 0


def weekInTop10(pos: int, date: str, coma = False):
    if firstTop10(pos) == 1:
        if coma:
            return "," + date
        return date
    return ""


def weekInTop25(pos: int, date: str, coma = False):
    if firstTop25(pos) == 1:
        if coma:
            return "," + date
        return date
    return ""


def weekInTop50(pos: int, date: str, coma = False):
    if firstTop50(pos) == 1:
        if coma:
            return ","+date
        return date
    return ""


def processData(mainDataframe):
    pass


def createFirstRow(cu_row):
    return {"ID": cu_row.Index,
            "TrackName": cu_row._2,
            "Artist": cu_row.Artist,
            "maxStreams": cu_row.Streams,
            "top1": cu_row.Position == 1,
            "timesTop10": firstTop10(cu_row.Position),
            "timesTop25": firstTop25(cu_row.Position),
            "timesTop50": firstTop50(cu_row.Position),
            "weeksTop10": weekInTop10(cu_row.Position, cu_row.startingDate),
            "weeksTop25": weekInTop25(cu_row.Position, cu_row.startingDate),
            "weeksTop50": weekInTop50(cu_row.Position, cu_row.startingDate),
            "accumulated_streams": cu_row.Streams
            }


def updatedRowData(main_row, cu_row):
    return {"ID": cu_row.Index,
            "TrackName": cu_row._2,
            "Artist": cu_row.Artist,
            "maxStreams": max(main_row.maxStreams, cu_row.Streams),
            "top1": main_row.top1 or cu_row.Position == 1,
            "timesTop10": firstTop10(cu_row.Position) + main_row.timesTop10,
            "timesTop25": firstTop25(cu_row.Position) + main_row.timesTop25,
            "timesTop50": firstTop50(cu_row.Position) + main_row.timesTop50,
            "weeksTop10": main_row.weeksTop10 + weekInTop10(cu_row.Position, cu_row.startingDate, True),
            "weeksTop25": main_row.weeksTop25 + weekInTop25(cu_row.Position, cu_row.startingDate, True),
            "weeksTop50": main_row.weeksTop50 + weekInTop50(cu_row.Position, cu_row.startingDate, True),
            "accumulated_streams": cu_row.Streams + main_row.accumulated_streams
            }


def processFile(cu: DataFrame, main: DataFrame):
    for cu_row in cu.itertuples():
        if cu_row.Index in main.index:
            main_row = main.loc[cu_row.Index]
            main.loc[cu_row.Index] = updatedRowData(main_row, cu_row)
        else:
            main.loc[cu_row.Index] = createFirstRow(cu_row)


def iterateFolder(folder: str, mainDataframe: DataFrame):
    for file in os.scandir(folder):
        current_dataframe = pd.read_csv(folder + "/" + file.name, encoding="utf-8")
        current_dataframe = current_dataframe.set_index(["ID"], verify_integrity=True)
        processFile(current_dataframe, mainDataframe)
    return mainDataframe


def createSingleCSV() -> DataFrame:
    return pd.DataFrame(
        columns=["TrackName", "Artist", "maxStreams", "avgStreams", "top1", "timesTop10",
                 "timesTop25", "timesTop50", "weeksTop10", "weeksTop25", "weeksTop50", "accumulated_streams"])


print("First param should be folder from where to read all CSV and merge them")
print("Example: global_weekly \r\n \r\n")
folder = argv[1]
if folderExist(folder) is False:
    SystemExit("The folder doents exits!")

mainDataframe = createSingleCSV()
iterateFolder(folder, mainDataframe)
processData(mainDataframe)
writeCsvFile("{0}_procesed.csv".format(folder), mainDataframe, True)
