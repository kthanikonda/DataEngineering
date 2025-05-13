# upload the files in colab
from google.colab import files
import shutil
import os

uploaded = files.upload()

file_name = next(iter(uploaded))
file_size = os.path.getsize(file_name)
shutil.move('acs2017_county_data.csv', '/content/acs2017_county_data.csv')
shutil.move('covid_confirmed_usafacts.csv', '/content/covid_confirmed_usafacts.csv')
shutil.move('covid_deaths_usafacts.csv', '/content/covid_deaths_usafacts.csv')

# Read and Trim
import pandas as pd
import numpy as np

census_df = pd.read_csv('/content/acs2017_county_data.csv')
cases_df = pd.read_csv('/content/covid_confirmed_usafacts.csv')
deaths_df = pd.read_csv('/content/covid_deaths_usafacts.csv')

cases_df = cases_df[['County Name','State','2023-07-23']]
deaths_df = deaths_df[['County Name','State','2023-07-23']]
census_df = census_df[['County','State','TotalPop','IncomePerCap','Poverty','Unemployment']]

print(f"The Covid Cases Columns: {cases_df.columns}")
print(f"The Covid Death Columns: {deaths_df.columns}")
print(f"The Census Data Columns: {census_df.columns}")

# Integration Challenge #1
cases_df['County Name'] = cases_df['County Name'].str.strip()
deaths_df['County Name'] = deaths_df['County Name'].str.strip()

washington_cases = cases_df[cases_df['County Name'] == "Washington County"]
washington_deaths = deaths_df[deaths_df['County Name'] == "Washington County"]

cases_count = len(washington_cases)
deaths_count = len(washington_deaths)

print(f"The Covid cases in the washington county: {cases_count}")
print(f"The deaths cases in the washington county: {deaths_count}")

# Integration Challenge #2
cases_df = cases_df[cases_df['County Name'] != "Statewide Unallocated"]
deaths_df = deaths_df[deaths_df['County Name'] != "Statewide Unallocated"]

print(f"Remaining rows in cases_df:", len(cases_df))
print(f"Remaining rows in deaths_df:", len(deaths_df))

# Integration Challenge #3
us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "Virgin Islands, U.S.": "VI",
}

# invert the dictionary
abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))

cases_df['State'] = cases_df['State'].replace(abbrev_to_us_state)
deaths_df['State'] = deaths_df['State'].replace(abbrev_to_us_state)

print(f"The First few rows of Covid Cases: \n{cases_df.head()}")

# Integration Challenge #4
cases_df['key'] = cases_df['County Name'] + ', ' + cases_df['State']
deaths_df['key'] = deaths_df['County Name'] + ', ' + deaths_df['State']
census_df['key'] = census_df['County'] + ', ' + census_df['State']

cases_df.set_index('key', inplace=True)
deaths_df.set_index('key', inplace=True)
census_df.set_index('key', inplace=True)

print(f"The First few rows of Census Cases: \n{census_df.head()}")

# Integration Challenge #5
cases_df = cases_df.rename(columns={'2023-07-23': 'Cases'})
deaths_df = deaths_df.rename(columns={'2023-07-23': 'Deaths'})

print(f"The column headers for cases: {cases_df.columns.values.tolist()}")
print(f"The column headers for deaths: {deaths_df.columns.values.tolist()}")

join_df = cases_df.join(deaths_df[['Deaths']], on='key')
join_df = join_df.join(census_df[['TotalPop']], on='key')

join_df['CasesPerCap'] = join_df['Cases'] / join_df['TotalPop']
join_df['DeathsPerCap'] = join_df['Deaths'] / join_df['TotalPop']

print(f"The no. of rows join_df contains: {len(join_df)}")

join_df = join_df.select_dtypes(include=['number'])

correlation_matrix = join_df.corr()
print(f"The Correlation matrix: \n{correlation_matrix}")

import seaborn as sns
import matplotlib.pyplot as plt

sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)

plt.title('Correlation Matrix Heatmap')
plt.show()

