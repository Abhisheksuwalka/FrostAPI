import pandas as pd
import numpy as np
import requests
import os

# 1. LOAD CREDENTIALS
from dotenv import load_dotenv
load_dotenv()
CLIENT_ID = os.getenv('CLIENTID')
CLIENT_SECRET = os.getenv('SECRET')



# 2. AUTHENTICATION
url = 'https://frost.met.no/auth/accessToken'
response = requests.post(url, data={
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'grant_type': 'client_credentials'
})
if response.status_code == 200:
    access_token = response.json()['access_token']
else:
    raise Exception(f"Failed to get access token: {response.status_code}")



# 3. TEST
referencetime = '2023-01-01/2023-12-31'
url = 'https://frost.met.no/observations/v0.jsonld'
headers = {'Authorization': f'Bearer {access_token}'}
source_ids = ["SN76935"]
params = {
  'sources': ','.join(source_ids),
  'referencetime': referencetime,
  'elements': 'air_temperature',
  'timeoffsets': 'default',
  'levels': 'default',
  'qualities': '0,1,2,3,4'
}
response = requests.get(url, headers=headers, params=params)
if response.status_code == 200:
    json_data = response.json()
    # print(json_data)
    print("Sample Data fetched successfully")
else:
    print(f"Failed to fetch data: {response.status_code}")






# element table : https://frost.met.no/elementtable
# some of the elements : 
"""
air_pressure,
relative_humidity
dew_point_temperature,
dew_point_height_estimated,
max(wind_speed PT10M),
max(wind_speed_of_gust PT10M),
max(wind_from_direction_of_gust PT10M),
max_wind_speed(wind_from_direction PT1H), # Wind direction at maximum mean wind speed
latitude,
longitude,
air_pressure_at_sea_level
mean(air_gap PT10M)
wind_from_direction
air_temperature
mean(air_temperature PT10M)
sea_surface_temperature
mean(sea_surface_temperature PT10M)
"""



element_list = ['air_temperature','relative_humidity','air_pressure','wind_speed','wind_from_direction','air_pressure_at_sea_level','dew_point_temperature','precipitation_amount','max(wind_speed_of_gust PT10M)', 'max(wind_speed PT10M)','cloud_area_fraction','visibility','mean(wind_speed P1H), dew_point_height_estimated', 'max(wind_from_direction_of_gust PT10M)', 'max_wind_speed(wind_from_direction PT1H)','latitude', 'longitude','air_pressure_at_sea_level','mean(air_gap PT10M)','wind_from_direction','mean(air_temperature PT10M)','sea_surface_temperature','mean(sea_surface_temperature PT10M)']

valid_element_list = ['wind_from_direction', 'air_pressure_at_sea_level', 'max(wind_from_direction_of_gust PT10M)', 'max(wind_speed_of_gust PT10M)', 'dew_point_temperature', 'air_pressure_at_sea_level', 'wind_from_direction', 'wind_speed', 'relative_humidity', 'air_temperature']

source_id_list = ['SN76935', 'SN76936', 'SN76968', 'SN76969', 'SN76962', 'SN76965', 'SN76959', 'SN76967', 'SN76957', 'SN76954']

for elementt in valid_element_list:
    print(f"- - - - - {elementt} - - - - -")
    # Fetch data for each year (e.g., 2005–2024)
    for year in range(2000, 2024):
        referencetime = f'{year}-01-01/{year}-12-31'
        url = 'https://frost.met.no/observations/v0.jsonld'
        headers = {'Authorization': f'Bearer {access_token}'}
        source_ids = source_id_list
        params = {
            'sources': ','.join(source_ids),
            'referencetime': referencetime,
            'elements': elementt,
            'timeoffsets': 'default',
            'levels': 'default',
            'qualities': '0,1,2,3,4'
        }
        
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            json_data = response.json()
            
            # json_data to df
            observations = []
            for item in json_data.get('data', []):
                source_id = item['sourceId']
                reference_time = item['referenceTime']
                for obs in item.get('observations', []):
                    observation = {
                        'source_id': source_id,
                        'reference_time': reference_time,
                        'element_id': obs['elementId'],
                        'value': obs['value'],
                        'unit': obs.get('unit', '')
                    }
                    observations.append(observation)
            df = pd.DataFrame(observations)
            if df.empty:
                print(f" - - - - - - - - - DataFrame is empty for year {year}")
            else:
                print(f"Retrieved data for {elementt}, {year}")
                # Save to CSV
                # if directory does not exist, create it
                if not os.path.exists(f'./data/{year}'):
                    os.makedirs(f'./data/{year}/')
                
                df.to_csv(f"./data/{year}/data_{elementt}_{year}.csv", index=False)
        else:
            print(f"Failed to fetch data: {response.status_code} for {elementt}, {year}")