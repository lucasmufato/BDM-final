from datetime import date, timedelta, datetime
from sys import argv

from pandas import DataFrame

from helper import createFolderIfItDoenstExist, createDataframeFromCsv, downloadFile, writeCsvFile

"""
EXAMPLE OF WHATS DOWNLOADED FROM THE API

,,,"Note that these figures are generated using a formula that protects against any artificial inflation of chart positions.",
Position,"Track Name",Artist,Streams,URL
1,"In My Feelings",Drake,43950715,https://open.spotify.com/track/0bAkKNCQfWkexHFn7fIKns
2,"Don’t Matter To Me",Drake,33681056,https://open.spotify.com/track/36ONiya0OANYknz0GgJmwB
3,SAD!,XXXTENTACION,28909080,https://open.spotify.com/track/3ee8Jmje8o58CHK66QrVC2

EXAMPLE OF URL TO QUERY

https://spotifycharts.com/regional/global/weekly/2018-07-06--2018-07-13/download

I'M STARTING THE SCRIPT THE DATE 2020-01-03. 
ALL PREVIUOS DATES WILL BE USED FOR TRAINING AND THE OTHER 2 MONTH FOR VALIDATION


"""


def getDateBefore(currentDate: date, mode: str) -> date:
    if mode == "weekly":
        return currentDate - timedelta(days=7)
    elif mode == "daily":
        return currentDate - timedelta(days=1)
    else:
        raise Exception("Le erraste con el parametro mode, es 'weekly' o 'daily")


def cutUntilLine(dataFrame: DataFrame, lines: int) -> DataFrame:
    return dataFrame[dataFrame.Position <= lines]


def transforUrlToId(dataFrame: DataFrame) -> DataFrame:
    lenghtToCut = len("https://open.spotify.com/track/")
    dataFrame['ID'] = dataFrame['URL'].map(lambda x: x[lenghtToCut:])
    return dataFrame.drop('URL', axis=1)


def addDateColumn(dataFrame: DataFrame, date: str) -> DataFrame:
    return dataFrame.assign(startingDate=date)


def download(country: str, mode: str, startingDate: date):
    baseUrl = "https://spotifycharts.com/regional/{0}/{1}".format(country, mode)
    currentDate = startingDate
    createFolderIfItDoenstExist("{0}_{1}".format(country, mode))
    while True:
        weekBefore = getDateBefore(currentDate, mode)
        url = "{0}/{1:%Y-%m-%d}--{2:%Y-%m-%d}/download".format(baseUrl, weekBefore, currentDate)
        print(url)
        file = downloadFile(url)
        if file is None:
            print("Last available date is {0:%Y-%m-%d}. EXITING SCRIPT".format(currentDate))
            return
        print("got the file")
        dataFrame = createDataframeFromCsv(file)
        del file, url
        dataFrame = cutUntilLine(dataFrame, 50)
        dataFrame = transforUrlToId(dataFrame)
        dataFrame = addDateColumn(dataFrame, "{0:%Y-%m-%d}".format(weekBefore))

        file_name = "{0}_{1}/{2:%Y-%m-%d}--{3:%Y-%m-%d}.csv".format(country, mode, weekBefore, currentDate)
        writeCsvFile(file_name, dataFrame)
        print("wrote down the file")
        currentDate = weekBefore


print("Example of params is: global weekly 2018-07-13")
print("where global is the country from where to get the list")
print("where weekly is the period of the list. it can be 'weekly' or 'daily'")
print("where 2018-07-13 is the starting date to scrap from, the scrip will start downloading from that "
      "date backwards untill it doenst find more csvs \r\n \r\n")
country, mode, date = argv[1], argv[2], argv[3]
date = datetime.strptime(date, "%Y-%m-%d").date()
download(country, mode, date)
