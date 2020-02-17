from sys import argv

import pandas as pd

from helper import writeCsvFile


def getDuplicates(dataset):
    duplicates = dataset[dataset.duplicated(["track_id"])]
    print("There are {0} duplicated songs.".format(duplicates.size))
    return duplicates


def deleteDuplicates(original):
    return original.drop_duplicates(["track_id"], keep="first")


originalDatasetPath = argv[1]
dataset = pd.read_csv(originalDatasetPath, encoding="utf-8")
print("Dataset has {0} songs".format(dataset.size))
duplicates = getDuplicates(dataset)
cleanDataset = deleteDuplicates(dataset)
print("New Dataset has {0} songs".format(cleanDataset.size))
writeCsvFile("cleanOriginal.csv", cleanDataset)
print("Done :)")