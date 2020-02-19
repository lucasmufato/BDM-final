from datetime import datetime
from sys import argv

import SpotifyScrapper
import dataSetCompleter
import merger

print("Example of params is: global weekly 2018-07-13 asdS-FSEF-EFESEF1A")
print("where global is the country from where to get the list")
print("where weekly is the period of the list. it can be 'weekly' or 'daily'")
print("where 2018-07-13 is the starting date to scrap from, the scrip will start downloading from that "
      "date backwards untill it doenst find more csvs \r\n")
print("The last param is the token which can be obtained from: https://developer.spotify.com/console/get-audio-features-track/?id=  \r\n \r\n")
country, mode, date, token = argv[1], argv[2], argv[3], argv[4]
date = datetime.strptime(date, "%Y-%m-%d").date()
print("")
print("")
print("SCRAPPING THE SONGS FROM THE TOPS")
print("")
SpotifyScrapper.download(country, mode, date)
print("")
print("")
print("MERGING DOWNLOADS TO ONE CSV")
print("")
merger.doMerge("{0}_{1}".format(country, mode))
print("")
print("")
print("COMPLETING CSV WITH METADATA")
print("")
dataSetCompleter.completeMergedDataset("out_merger/{0}_{1}_procesed.csv".format(country, mode),token)
print("")
print("FINISHED")
print("")
