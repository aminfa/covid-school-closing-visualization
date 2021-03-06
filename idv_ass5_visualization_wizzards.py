'''
authors:
    Darya Zarkalam  6830692
    Salma Charfi    6812168
    Amin Faez       7032123
    Florian Nutt    7037636
'''

# imports
try:
    import pandas as pd
    import plotly.express as px
except Exception:
    print("Please install pandas and plotly:\n{}\n{}".format("pip install pandas==1.0.5", "pip install plotly==4.8.1"))
    exit(1)
import csv
from datetime import datetime
from math import log
import sys, os
import os.path as path

input_folder = "Data"

if len(sys.argv) == 1:
    print("The data folder isn't specified; falling back to default data folder: `{}`."
          " You may define it in the command line: {}".format(
        input_folder,
        "python idv_ass5_visualization_wizzards.py custom/data/folder"
    ))
else:
    input_folder = sys.argv[1]
    if not path.isdir(input_folder):
        print("The specified input directory doesn't exist: {}".format(input_folder))
        exit(1)
    elif not path.isfile(input_folder + "/owid-covid-data.csv"):
        print("The specified input directory doesn't contain the owid covid data: {}".format(input_folder + "/owid-covid-data.csv"))
        exit(1)
    elif not path.isfile(input_folder + "/school-closures-covid.csv"):
        print("The specified input directory doesn't contain the school closure data: {}".format(input_folder + "/school-closures-covid.csv"))
        exit(1)

if not path.isdir(input_folder):
    os.mkdir(input_folder)

# fetch from github
if not path.isfile(input_folder + "/owid-covid-data.csv"):
    try:
        import urllib.request
        print('Beginning dataset download to {}...'.format(input_folder + "/owid-covid-data.csv"))
        url = 'https://github.com/aminfa/covid-school-closing-visualization/raw/master/Data/owid-covid-data.csv'
        urllib.request.urlretrieve(url, input_folder + "/owid-covid-data.csv")
    except Exception as ex:
        print("The specified input directory doesn't contain the owid covid data: {}".format(input_folder + "/owid-covid-data.csv"))
        exit(1)
if not path.isfile(input_folder + "/school-closures-covid.csv"):
    try:
        import urllib.request
        print('Beginning dataset download to {}...'.format(input_folder + "/school-closures-covid.csv"))
        url = "https://raw.githubusercontent.com/aminfa/covid-school-closing-visualization/master/Data/school-closures-covid.csv"
        urllib.request.urlretrieve(url, input_folder + "/school-closures-covid.csv")
    except Exception as ex:
        print("The specified input directory doesn't contain the school closure data: {}".format(input_folder + "/school-closures-covid.csv"))
        exit(1)

# `data` is a 2 dimensional array that is going to contain all extracted and processed data.
# Each row describes the covid and school situation of a country in one day.
# e.g. data[0] might be: ['AFG', 'Asia', 'Afghanistan', '2019-12-31', '0.0']
data = []

# Read the covid data using the `csv` library. Then extract only those columns that are necessary and add them to `data`.
with open(input_folder + "/owid-covid-data.csv", "r") as covid_csv:
    reader = csv.reader(covid_csv, delimiter=',')
    head = reader.__next__()
    body = []
    for row in reader:
        body.append(row)

needed_columns = ["iso_code", "continent", "location", "date", "new_cases_per_million"]
needed_columns_index = []

for nc in needed_columns:
    nc_index = head.index(nc)
    needed_columns_index.append(nc_index)

print("The columns needed `{}` are indexed with: `{}`".format(needed_columns, needed_columns_index))

for row in body:
    extracted_row = []
    for index in needed_columns_index:
        extracted_row.append(row[index])
    data.append(extracted_row)

print("Extracted data (first three rows):\n{}\n".format(data[:3]))
del head, body, needed_columns_index

# Now read the school closing data and then merge it with `data` by doing a join operation
with open(input_folder + "/school-closures-covid.csv", "r") as school_csv:
    reader = csv.reader(school_csv, delimiter=',')
    head = reader.__next__()
    body = []
    for row in reader:
        body.append(row)

school_data = body

# Apply join on iso_code and date.
# i.e. as SQL:
# SELECT "iso_code", "continent", "location", "date", "new_cases_per_million", "School closures"
# FROM data, school_data
# WHERE data."iso_code" == school_data."Code" AND data."date" == school_data."Date"

# Inorder to increase the speed of join, index data by its column "date"
# Indexing is done by creating a dict that map each day to a list of rows:

column_index_date_covid = 3

data_date_dict = {}
for row in data:
    row_date = row[column_index_date_covid]
    parsed_date = datetime.strptime(row_date, "%Y-%m-%d")
    if parsed_date not in data_date_dict:
        data_date_dict[parsed_date] = []

    data_date_dict[parsed_date].append(row)

# Run join operation and delete ununsed fields:

column_index_date_school = 2
column_index_isocode_school = 1
column_index_closures_school = 3

column_index_date_covid = 3
column_index_isocode_covid = 0
for row_school in school_data:
    # Go through date rows:
    row_school_date = row_school[column_index_date_school]
    # parse the date in school closures to match the dates in the covid dataset.
    # The covid dataset has dates like: 2019-12-31
    # The school dataset has dates like: Jan 1, 2020
    row_date = datetime.strptime(row_school_date, '%b %d, %Y')
    if row_date not in data_date_dict:
        print("There is no date `{}` in the covid dataset.".format(row_school_date))
    for rows_with_matching_date in data_date_dict[row_date]:
        if rows_with_matching_date[column_index_isocode_covid] == row_school[column_index_isocode_school]:
            # add school closure to `data`
            rows_with_matching_date.append(row_school[column_index_closures_school])

print("Joined data (first three rows):\n{}\n".format(data[:3]))
del head, body, school_data


columns = ["ISO-code", "Continent", "Location", "Date", "New Cases per Million", "school_status_code", "circle_size", "School Status"]

# This function defines the translation between color code and color text:
color_status_texts = ["No Data", "No Restriction Measures", "Recommended Closing", "Required Closing at Some Levels" ,"Required Closing at All Levels"]
def get_circle_color(color_code):
    if color_code is None or color_code == "":
        return "No Data"
    color_code = str(color_code)
    if  color_code == "0":
        return "No Restriction Measures"
    elif color_code == "1":
        return "Recommended Closing"
    elif color_code == "2":
        return "Required Closing at Some Levels"
    elif color_code == "3":
        return "Required Closing at All Levels"

# Lets format the columns
rows_remaining = []
logs = {}

min_date = datetime(2020, 3, 1)
max_date = datetime(2020, 7, 1)

date_registry = []

for row in data:
    try:
        if len(row) == 5:
            # School status is missing:
            row.append("")  # None signals missing value
        elif len(row) != 6:
            # Row length is weird:
            raise Exception("illegal-row-dimension")

        # Filter empty values
        for value in row[:-2]:
            if value is None or len(value) == 0:
                raise Exception("Empty value")
                pass

        # format date.
        # It is present as a string like `2019-12-31`
        # Transform it to: Dec, 31
        row_date = row[3]
        date = datetime.strptime(row_date, "%Y-%m-%d")
        if date < min_date or date > max_date:
            raise Exception("DateOutOfBound")
        if row_date not in date_registry:
            date_registry.append(row_date)


        # format new cases
        new_cases = row[4]
        if len(new_cases) == 0:
            new_cases = '0'  # assume its 0
        new_cases_float = float(new_cases)  # parse as float
        if new_cases_float < 0:
            row[4] = 0
            row[5] = ""
            # raise Exception("Negative value")
        # add 100 to all new cases:
        row[4] = new_cases_float

        # append circle size:
        circle_size = 0.5 # min size
        circle_size += log(row[4] + 1)
        row.append(circle_size)

        # append circle color:
        row.append(get_circle_color(row[5]))

        rows_remaining.append(row)
    except Exception as ex:
        print("Couldn't format row: `{}`\n Error:".format(row), ex)
        # traceback.print_exc()
        message = str(ex)
        if message not in logs:
            logs[message] = []
        logs[message].append(row)

# save deleted rows:
for message in logs.keys():
    with open(input_folder + "/deleted{}.csv".format(message), "w") as log_f:
        wr = csv.writer(log_f, quoting=csv.QUOTE_ALL)
        wr.writerows(logs[message])

data = rows_remaining

# add 0 sized circle for each color category for each date:
inivisible_country = {
    "No Data": "MHL", # Marshall Islands
    "No Restriction Measures" : "MTQ",
    "Recommended Closing" : "NIU",
    "Required Closing at Some Levels" : "TUV",
    "Required Closing at All Levels" : "WSM"
}
date_registry.sort()
for date in date_registry:
    for code, country in inivisible_country.items():
        # columns = ["ISO-code", "Continent", "Location", "Date", "New Cases per Million", "school_status_code", "circle_size", "School Status"]
        data.append([country, "foo", "foo", date, 0, 0, 0.2, code])

# Write the resulting file the the data folder:
with open(input_folder + "/resulting_table.csv", "w") as result_f:
    wr = csv.writer(result_f, quoting=csv.QUOTE_ALL)
    wr.writerows(data)


# Lets transform data to a dataframe and present it using plotly
df = pd.DataFrame(data, columns=columns)
df = df.sort_values(by=["Date", "Location"])
hover_columns = ["Location", "Date", "New Cases per Million", "School Status"]
info_box_values = df[hover_columns]
fig = px.scatter_geo(df,
                     locations=columns[0],  # map position (iso code)
                     size=columns[-2],  # circle size
                     color=columns[-1],  # circle color
                     # color_discrete_sequence=color_status_texts,
                     hover_name=columns[2],  # Location
                     hover_data=info_box_values,
                     # location, date, new cases per million, circle color # TODO change column name
                     animation_frame=columns[3],  # Date
                     # animation_group=columns[-1],
                     size_max=10,  # circle max size
                     projection="natural earth")
fig.update_layout(
    title={
        'text': "Measures on educational institutes in combination with sum of new cases per million in the last 7 days",
        'y': 1,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    }
)

fig.show()
