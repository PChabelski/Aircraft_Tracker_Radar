import json
import pandas as pd
import os



with open(f"aircraft_dictionary.json", 'r') as file:
    aircraft_dictionary = json.loads(file.read())

df_aircraft = pd.DataFrame.from_dict(aircraft_dictionary, orient='index')
df_aircraft = df_aircraft.rename_axis('hexcode').reset_index()
df_aircraft['hexcode'] = df_aircraft['hexcode'].astype('str') # force as a string
df_aircraft.to_csv('Aircraft_Register.csv', index=False)
