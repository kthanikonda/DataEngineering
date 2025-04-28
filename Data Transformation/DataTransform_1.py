import pandas as pd


df = pd.read_csv('bc_veh4223_230215.csv')

df.columns = df.columns.str.strip()

df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

df['TIMESTAMP'] = df['OPD_DATE'] + pd.to_timedelta(df['ACT_TIME'], unit='s')

df['dMETERS'] = df['METERS'].diff()
df['dTIMESTAMP'] = df['TIMESTAMP'].diff()

df['SPEED'] = df.apply(
    lambda row: row['dMETERS'] / row['dTIMESTAMP'].total_seconds() if pd.notna(row['dTIMESTAMP']) else None,
    axis=1
)

trip_stats = df.groupby('EVENT_NO_TRIP')['SPEED'].agg(['max', 'median'])

max_speed_trip = trip_stats.loc[trip_stats['max'].idxmax()]
max_speed_value = max_speed_trip['max']

max_speed_record = df[df['SPEED'] == max_speed_value].iloc[0]

print(f"Maximum speed for vehicle #4223 on February 15, 2023: {max_speed_value} m/s")
print(f"Location Longitude, Latitude and time of maximum speed: ({max_speed_record['GPS_LONGITUDE']}, {max_speed_record['GPS_LATITUDE']}) at {max_speed_record['TIMESTAMP']}")
print(f"Median speed for vehicle #4223 on February 15 2023: {trip_stats['median'].median()} m/s")
