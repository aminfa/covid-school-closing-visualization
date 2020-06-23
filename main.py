'''
authors:
    Darya Zarkalam  6830692
    Salma Charfi    6812168
    Amin Faez       7032123
    Florian Nutt    7037636
'''

#imports
import pandas as pd
import plotly.express as px

#read in files
dfCovid = pd.read_csv("Data/owid-covid-data.csv", delimiter=",")
dfSchools = pd.read_csv("Data/school-closures-covid.csv", delimiter=",")
#drop columns
CovidCloumnList = ["iso_code","continent","location","date","new_cases_per_million"]
dfCovid = dfCovid[CovidCloumnList]
# remove international and worls rows
dfCovid = dfCovid[dfCovid.location != "International"]
dfCovid = dfCovid[dfCovid.location != "World"]
#transform date column to Datetype, to be able to join datas frames
dfSchools["Date"] = pd.to_datetime(dfSchools.Date)
dfCovid["date"] = pd.to_datetime(dfCovid.date)
#rename cloumns
dfSchools.rename(columns={"Code": "iso_code","Date": "date"}, inplace = True)
#join dataframes
dfTotal = pd.merge(dfCovid, dfSchools,  on=["iso_code","date"])
#remove nan values and convert negative values to positive
dfTotal["new_cases_per_million"] = dfTotal["new_cases_per_million"].fillna(0)
dfTotal["new_cases_per_million"] = dfTotal["new_cases_per_million"].astype(str)
dfTotal["new_cases_per_million"] = dfTotal["new_cases_per_million"].str.replace("-","")
#convert date to string. Datetype cant be used for the visualization -> throws error
dfTotal["date"] = dfTotal["date"].astype(str)
dfTotal["new_cases_per_million"] = dfTotal["new_cases_per_million"].astype(float)

#save preprocessed and merged data
dfTotal.to_csv("Data/covidMerged.csv", sep=',')
print(dfTotal.dtypes)

#working example
'''
df = px.data.gapminder()
fig = px.scatter_geo(df, locations="iso_alpha", color="continent",
                     hover_name="country", size="pop",
                     animation_frame="year",
                     projection="natural earth")
fig.show()


'''
fig = px.scatter_geo(dfTotal, locations="iso_code", color="School closures",
                     hover_name="location", size="new_cases_per_million",
                     animation_frame="date",
                     projection="natural earth")
fig.show()
