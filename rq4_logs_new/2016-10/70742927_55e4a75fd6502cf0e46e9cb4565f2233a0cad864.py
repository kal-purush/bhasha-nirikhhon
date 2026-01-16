__author__ = "Ben Nelsen"

import backwaterLength


s_folderPath = "C:\\Users\\bnelsen2\\Documents\\SeniorYearOne\\CEEN433"
s_fileName = "backwaterConfigFile.txt"

backwater = backwaterLength.Channel()
backwater.readConfigurationfile(s_folderPath, s_fileName)
backwater.backwaterDetermination()
