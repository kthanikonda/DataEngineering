import pandas as pd

df = pd.read_csv('bc_trip259172515_230215.csv')

print("Column names before dropping any columns:")
print(df.columns)

df = df.drop(columns=['EVENT_NO_STOP', 'GPS_SATELLITES'], errors='ignore')

print("DataFrame after dropping 'EVENT_NO_STOP' and 'GPS_SATELLITES' columns:")
print(df.head())

df_filtered = pd.read_csv('bc_trip259172515_230215.csv', usecols=lambda col: col not in ['EVENT_NO_STOP', 'GPS_SATELLITES'])

print("DataFrame after loading data with usecols to filter columns:")
print(df_filtered.head())

df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')
df['TIMESTAMP'] = df['OPD_DATE'] + pd.to_timedelta(df['ACT_TIME'], unit='s')

print("OPD_DATE, ACT_TIME, and TIMESTAMP for the first few records:")
print(df[['OPD_DATE', 'ACT_TIME', 'TIMESTAMP']].head())

df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])

print("DataFrame after dropping OPD_DATE and ACT_TIME columns:")
print(df.head())

df['dMETERS'] = df['METERS'].diff()
df['dTIMESTAMP'] = df['TIMESTAMP'].diff()

df['SPEED'] = df.apply(
    lambda row: row['dMETERS'] / row['dTIMESTAMP'].total_seconds() 
    if pd.notna(row['dTIMESTAMP']) else None, axis=1
)

df = df.drop(columns=['dMETERS', 'dTIMESTAMP'])

min_speed = df['SPEED'].min()
max_speed = df['SPEED'].max()
avg_speed = df['SPEED'].mean()

print(f"Minimum Speed: {min_speed} m/s")
print(f"Maximum Speed: {max_speed} m/s")
print(f"Average Speed: {avg_speed} m/s")
