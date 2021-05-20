# Authors - Karanjit Singh, Rishabh Manish Sahlot, Tejas Patel
from myutils import hourToLabelEncoder
import pandas as pd
import math
from datetime import datetime
from pymongo import MongoClient

# Initializing the Folder paths
raw_data_path = 'Raw_Data/'
processed_data_path = 'Processed_Data/'
links_data_path = 'Data_Links/'
# Reading the df_nypd data
df_nypd = pd.read_csv(raw_data_path+'nypd_data.csv')
# Finding the year and month of crime for the nypd data
df_nypd['year of crime'] = list(map(lambda x: str(x.split('/')[2]), df_nypd['ARREST_DATE'].values))
df_nypd['month of crime'] = list(map(lambda x: str(x.split('/')[0]), df_nypd['ARREST_DATE'].values))
# Aggregating the number of arrests for each precinct
crime_counts = df_nypd.groupby('ARREST_PRECINCT').agg(['count'])['ARREST_KEY']
# Saving the new df_nypd file in the porcessed data as well as removing it from the memory
# Since it will not be used the preprocessing henceforth.
df_nypd.to_csv(processed_data_path +'nypd_data.csv')
del df_nypd
# Reading the precinct lookup table
df_taxi_with_precint = pd.read_csv(links_data_path+'taxi_with_precint.csv')
# Correcting the pipe separated values to array elements in the 'Corresponding_Taxi_Zones' column
df_taxi_with_precint['Corresponding_Taxi_Zones'] = list( map(lambda x: list(map(int,x.split('|'))),df_taxi_with_precint['Corresponding_Taxi_Zones'].values))
# merging the counts of arrest for each precint and the Corresponding Taxi Zones for the Precint
crime_counts = crime_counts.merge(df_taxi_with_precint, left_on='ARREST_PRECINCT', right_on='NYPD Precinct')
# Saving (in the processed Folder) and deleting(from the program cache) the precinct lookup
# since it's use has been over.
df_taxi_with_precint.to_csv(processed_data_path +'taxi_with_precint.csv')
del df_taxi_with_precint
# Assigning crime rate labels for hardcoded(using visualization) values of arrest counts
crime_counts['CrimeRate'] = 1
crime_counts['CrimeRate'][crime_counts['count'] < 45000] = 'Low Crime'
crime_counts['CrimeRate'][(crime_counts['count'] >= 45000) & (crime_counts['count'] < 90000)] = 'Medium Crime'
crime_counts['CrimeRate'][crime_counts['count'] >= 90000] = 'High Crime'
# Flattening / unwinding the crime counts to get each taxi zone
crime_counts = crime_counts.explode('Corresponding_Taxi_Zones')

## Moving on to University Data
# Reading the university data
df_uni = pd.read_csv(raw_data_path+'cuny_locations.csv')
# The above data is clean and should be saved and deleted from memory since it has no later significance
df_uni.to_csv(processed_data_path +'cuny_location.csv')
del df_uni
# Reading Univeristy Lookup Table as well as saving it to it's corresponding Location
df_uni_lookup = pd.read_csv(links_data_path+'taxi_with_cuny.csv')
df_uni_lookup.to_csv(processed_data_path+'taxi_with_cuny.csv')

#Moving on to the taxi Data
# Reading the taxi zone lookup data
df_taxi_zone_lookup = pd.read_csv(links_data_path+'taxi_zone_lookup.csv')
# Since it contains all the location ID's in the taxi data, we can do the merging
# operation to this one- this saves a lot of time and program memory
df_taxi_zone_lookup = pd.merge(df_taxi_zone_lookup, crime_counts[['Corresponding_Taxi_Zones','CrimeRate']], left_on='LocationID', right_on='Corresponding_Taxi_Zones', how='left')
df_taxi_zone_lookup = pd.merge(df_taxi_zone_lookup, df_uni_lookup, left_on='LocationID', right_on='Location ID', how='left')
# Freeing space of the non-essential merged items
del crime_counts
del df_uni_lookup
#Now using the College ID data obtained from we add the HasSchool feature
df_taxi_zone_lookup['HasSchool'] = 'false'
df_taxi_zone_lookup['HasSchool'][df_taxi_zone_lookup['College ID'].notna()] = 'true'
# We then drop of waste columns
del df_taxi_zone_lookup['Location ID']
del df_taxi_zone_lookup['Corresponding_Taxi_Zones']
del df_taxi_zone_lookup['College ID']
del df_taxi_zone_lookup['Zone']
del df_taxi_zone_lookup['service_zone']

# Finally we read the taxi trip data
df_taxi = pd.read_csv(raw_data_path+'yellowtaxi_data.csv')
# First and foremost we clean the data
# 1. Remove All values with trip distance less than or equal to 0
df_taxi = df_taxi.drop(df_taxi[df_taxi['trip_distance'] <= 0].index)
# 2. Remove All values with total amount less than 0
df_taxi = df_taxi.drop(df_taxi[df_taxi['total_amount'] <= 0].index)
# 3.
df_taxi = df_taxi.drop(df_taxi[df_taxi['extra'] <= 0].index)
# Fix the date time columns
df_taxi['tpep_pickup_datetime'] = list(map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),df_taxi['tpep_pickup_datetime'].values))
df_taxi['tpep_dropoff_datetime'] =list(map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),df_taxi['tpep_dropoff_datetime'].values))
# Calculate trip duration in hours
df_taxi['Trip duration'] = list(map(lambda x: x.item()/(3600*(10**9)),(df_taxi['tpep_dropoff_datetime']-df_taxi['tpep_pickup_datetime']).values))
# Incidently I found that trip duration becomes negative in some cases i.e. pickup happening after drop off, we dont try to analyze these values and simply eliminate them
df_taxi = df_taxi.drop(df_taxi[df_taxi['Trip duration'] <= 0].index)

# Calculating average speed during the trip
df_taxi['Speed'] = df_taxi['trip_distance']/df_taxi['Trip duration']
# Thresholding for speed to predict traffic such lower speed of vehicles implies higher traffic
df_taxi['Traffic'] = 0
df_taxi['Traffic'][df_taxi['Speed']<=10] = 'High Traffic'
df_taxi['Traffic'][(df_taxi['Speed']>10) & (df_taxi['Speed']<=25)] = 'Medium Traffic'
df_taxi['Traffic'][df_taxi['Speed']>25] = 'Low Traffic'
# Calculating Hour the the dat HoD for the dropoff (DOHoD) & pickup values (PUHoD)
df_taxi['DOHoD'] = df_taxi['tpep_dropoff_datetime'].astype('datetime64[ns]').dt.hour
df_taxi['PUHoD'] = df_taxi['tpep_pickup_datetime'].astype('datetime64[ns]').dt.hour
# Calculating the respective Time Codes using these values
df_taxi['DOTimeCode'] = list(map(lambda x: hourToLabelEncoder(x), df_taxi['DOHoD'].values))
df_taxi['PUTimeCode'] = list(map(lambda x: hourToLabelEncoder(x), df_taxi['PUHoD'].values))
# Finally Merging All the Data From the combined taxi zone lookup data
df_taxi = pd.merge(df_taxi, df_taxi_zone_lookup, left_on='PULocationID', right_on='LocationID',how='left')
#Getting columns of the comibined values
exterior_columns = df_taxi_zone_lookup.columns
# Changing names in dt_taxi to indicate they are for pickup location
for cols in exterior_columns:
    df_taxi.rename(columns={cols: 'PU'+cols}, inplace=True)
# Repeating the same with DO locations
df_taxi = pd.merge(df_taxi, df_taxi_zone_lookup, left_on='DOLocationID', right_on='LocationID',how='left')
#Storing & Deleting the combined values
df_taxi_zone_lookup.to_csv(processed_data_path+'taxi_zone_lookup.csv')
del df_taxi_zone_lookup
# Changing names in dt_taxi to indicate they are for pickup location
for cols in exterior_columns:
    df_taxi.rename(columns={cols: 'DO'+cols}, inplace=True)

# Finally storing the taxi data at it's require position
df_taxi.to_csv(processed_data_path+'yellowtaxi_data.csv')



# Uploading the final table to mongoDB
# Creates a client connection to the default MongoDB database as 27017
client = MongoClient()
# Estabilishing a connection with an already existing database named taxi_data
db = client['taxi_data']
# Selecting the collection/table yellowtaxi_data
coll = db['yellowtaxi_data']
coll.drop()
coll = db['yellowtaxi_data']


df_taxi.reset_index(inplace=True)
# Rename first column to ID so as to ensure easier retrieval using the primary key of the table
df_taxi.rename(columns={df_taxi.columns[0]: '_id'}, inplace=True)
# Getting dictionary
df_taxi = df_taxi.to_dict("records")
# Inserting the main document in mongodb database
coll.insert_many(df_taxi)




