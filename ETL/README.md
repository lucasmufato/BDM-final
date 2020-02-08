# Section about ETL process

### Scripts
* helper --> contains several functions common to all scripts
* SpotifyScrapper --> downloads the top 200 of a determined country in a period given a starting day
* merger --> merges all csv in a folder to create a single csv containing more information
* dataSetCompleter --> completes the starting dataset with the song that got in the top for a specified period

### How to use

* 1 - create virtual environment
* 2 - pip install requirements.txt
* 3 - run SpotifyScrapper with parameters: _global weekly 2020-01-03_
* 4 - run merger with parameters: global_weekly
* 5 - TODO..