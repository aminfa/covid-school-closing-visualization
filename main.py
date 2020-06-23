'''
authors:
    Darya Zarkalam  6830692
    Salma Charfi    6812168
    Amin Faez       7032123
    Florian Nutt    7037636
'''

#imports
import pandas as pd

#read in files
dfCovid = pd.read_csv("Data/owid-covid-data.csv", delimiter=",")
dfSchools = pd.read_csv("Data/school-closures-covid.csv", delimiter=",")
#drop columns
CovidCloumnList = ["iso_code","continent","location","date","total_cases_per_million"]
dfCovid = dfCovid[CovidCloumnList]
# remove international and worls rows
dfCovid = dfCovid[dfCovid.location != "International"]
dfCovid = dfCovid[dfCovid.location != "World"]
#transform date column
dfSchools["Date"] = pd.to_datetime(dfSchools.Date)
dfCovid["date"] = pd.to_datetime(dfCovid.date)
#rename cloumns
dfSchools.rename(columns={"Code": "iso_code","Date": "date"}, inplace = True)
#join dataframes
dfTotal = pd.merge(dfCovid, dfSchools,  on=["iso_code","date"])
#save preprocessed and merged data
dfTotal.to_csv("Data/covidMerged.csv", sep=',')