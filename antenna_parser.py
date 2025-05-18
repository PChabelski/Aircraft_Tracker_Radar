# import urllib library
from urllib.request import urlopen
import time
import json
import math
import re
import datetime
import requests
import numpy as np
from requests import get
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
flight_tracker = {}
import glob
breakout = True
import pandas as pd
import traceback
timer = 1
current_set = []
flying_hex = []
no_flight_check = {}
fail_counter = 0
diagnostic_count = 0
response_avg = 0
json_avg = 0
distance_avg = 0
response_max = 0
json_max = 0
distance_max = 0
hex_max = 0
time_taken_hex_parse = 0
'''
This is a scrubbed-down version of the code that I am currently running on my machine
This version aims to simply that beast, such that we only parse the furthest distance for each unique aircraft
that gets spotted by the antenna. 


'''

## URL -> removing the hardcoded IP address, reading from a json that is ignored by git
with open(f"private/private.json", 'r') as file:
    creds = json.loads(file.read())
url_ip = creds['url_ip']
url = f"http://{url_ip}/dump1090/data/aircraft.json"
##################################################################################

print('Start!')
# This thing will run until we decide to turn it off
while True:
    try:
        #print(f'[{time.ctime()}] The cycle has started again')
        # Read the configuration file that dictates how often to ping the antenna
        # Check if the status is still set to RUN. If it isn't, break out of the loop and end the process.
        # If it is, then we will ping the antenna and get the data, then sleep for a period of time
        with open(f"frequency_config.json", 'r') as file:
            config = json.loads(file.read())
        if config['status'] != 'RUN':
            print(f"[{time.ctime()}] Status is not set to RUN. Exiting...")
            break
        time_sleep = config['time_sleep']
        time.sleep(time_sleep)

        # define metadata for this run
        new_set = []
        air_count = 0
        timenow = datetime.datetime.fromtimestamp(time.time())
        date = timenow.date().strftime("%m-%d-%Y")
        hexcode = ''

        # The fun part - go through the dump1090 data and attempt to parse it
        # If the wifi connection to the raspberry pi is lost, then we will get a 404 error... so we will keep trying
        try:
            response = requests.head(url, timeout=30)
            if response.status_code == 200:
                data_json = get(url, timeout=2).json()

            elif response.status_code == 404:
                print("File not found.")
                raise Exception

            else:
                print(f"An unexpected status code was returned: {response.status_code}")
                raise Exception

        except Exception as e:
            print(f'[{time.ctime()}] Likely timed out')
            raise e

        # Check what aircraft are on the radar
        airborne_planes = data_json['aircraft']
        on_radar = len(airborne_planes)
        print(f"[{time.ctime()}] ...On radar: {on_radar} aircraft")

        # if our antenna picked up a flight(s) broadcast...
        if len(airborne_planes) > 0:
            # for each aircraft in that broadcast...
            for plane_cnt in range(0, len(airborne_planes)):
                # extract the hexcode (this is effectively the unique identifier for the aircraft)
                hexcode = airborne_planes[plane_cnt]['hex']
                # seen_pos is the key metric - we want to ensure that there are co-ordinates available for the aircraft
                if ('seen_pos' in airborne_planes[plane_cnt]):
                    # Try to extract the flight number, if it exists
                    try:
                        flight = airborne_planes[plane_cnt]['flight'].strip()
                    except:
                        flight = ''

                    # if the position data is fresh (60s is a good number)
                    if airborne_planes[plane_cnt]['seen_pos'] < 60:
                        lon = airborne_planes[plane_cnt]['lon']
                        lat = airborne_planes[plane_cnt]['lat']
                        altitude = airborne_planes[plane_cnt]['altitude']

                        # read the aircraft_dictionary that we will update/read from
                        with open(f"aircraft_dictionary.json", 'r') as file:
                            aircraft_dictionary = json.loads(file.read())
                        flight_register = {}

                        if hexcode in aircraft_dictionary.keys():
                            #print(f'[{time.ctime()}] {hexcode} found in file list')
                            new_flight_status = False
                            new_flight_str = ''
                            airline = aircraft_dictionary[hexcode]['airline']
                            registration = aircraft_dictionary[hexcode]['registration']
                            aircraft = aircraft_dictionary[hexcode]['aircraft']
                            aircraft_thumb = aircraft_dictionary[hexcode]['aircraft_thumbnail']
                        else:
                            #print(f'[{time.ctime()}] {hexcode} NOT found in file list')
                            new_flight_status = True
                            new_flight_str = 'New Aircraft |'
                            # parse the hexdbio API to get the full metadata for the aircraft
                            hexUrl = f'https://hexdb.io/api/v1/aircraft/{hexcode}'
                            aircraft_data = get(hexUrl, timeout=5).json()
                            try:
                                airline = aircraft_data['RegisteredOwners']
                                registration = aircraft_data['Registration']
                                aircraft = aircraft_data['Type']
                                thumb_url = f'https://hexdb.io/hex-image-thumb?hex={hexcode}'
                                aircraft_thumb_encoded = requests.get(thumb_url).content
                                aircraft_thumb = aircraft_thumb_encoded.decode("utf-8")
                                # If we can't find an image, we will default to a generic image
                                if aircraft_thumb == 'n/a':
                                    aircraft_thumb = 'https://upload.wikimedia.org/wikipedia/commons/thumb/1/17/Plane_icon_nose_up.svg/248px-Plane_icon_nose_up.svg.png'
                                else:
                                    try:
                                        aircraft_thumb = 'https://' + aircraft_thumb.split('//')[1]
                                    except:
                                        # sometimes this breaks; just default when it does
                                        aircraft_thumb = 'https://upload.wikimedia.org/wikipedia/commons/thumb/1/17/Plane_icon_nose_up.svg/248px-Plane_icon_nose_up.svg.png'

                            except KeyError:
                                airline = 'Unknown Airline'
                                registration = 'Unknown Registration'
                                aircraft = 'Unknown Aircraft Type'
                                aircraft_thumb = 'https://upload.wikimedia.org/wikipedia/commons/thumb/1/17/Plane_icon_nose_up.svg/248px-Plane_icon_nose_up.svg.png'

                        # extract whatever other juicy data is available
                        try:
                            speed = airborne_planes[plane_cnt]['speed']
                        except:
                            speed = np.nan
                        try:
                            vert_rate = airborne_planes[plane_cnt]['vert_rate']
                        except:
                            vert_rate =np.nan
                        try:
                            track = airborne_planes[plane_cnt]['track']
                        except:
                            track = np.nan

                        # so we don't have to keep checking the same hexcode over and over again
                        if hexcode in flying_hex:
                            #print(f'[{time.ctime()}] {air_count} {hexcode} hexcode is currently flying and captured: ', flying_hex)
                            pass
                            # continue # if you want to keep the single value; pass if you want all
                        else:
                            #print(f'[{time.ctime()}] {hexcode} hexcode isgoing to be added to our flying count: ', flying_hex)
                            flying_hex.append(hexcode)
                            first_seen_time = time.ctime()

                        # extra metadata stuff - can likely pull out into separate functions and or configs
                        if 'Unknown Aircraft Type' in aircraft:
                            unknown_status=True
                            unknown_str = 'Unknown |'
                        else:
                            unknown_status=False
                            unknown_str = ''
                        if ('Force' or 'Marine' or 'Military' or 'Army' or 'Government' or 'Navy') in airline:
                            military_status=True
                            military_str = 'Military |'
                        else:
                            military_status = False
                            military_str = ''
                        if ('747' or 'A380') in aircraft:
                            jumbo_status=True
                            jumbo_str = 'JUMBO |'
                        else:
                            jumbo_status = False
                            jumbo_str = ''

                        decoded_dest = ''
                        decoded_origin = ''
                        seen_pos = ' '
                        air_count += 1

                        airborne_planes[plane_cnt]['airline'] = airline
                        airborne_planes[plane_cnt]['registration'] = registration
                        airborne_planes[plane_cnt]['aircraft'] = aircraft
                        airborne_planes[plane_cnt]['flight'] = flight

                        home_lon = math.radians(creds['home_lon'])
                        home_lat = math.radians(creds['home_lat'])
                        flight_lat = math.radians(lat)
                        flight_lon = math.radians(lon)

                        dlon = home_lon - flight_lon
                        dlat = home_lat - flight_lat

                        a = (math.sin(dlat / 2)) ** 2 + math.cos(lat) * math.cos(home_lat) * (math.sin(dlon / 2)) ** 2
                        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                        R = 6373.0
                        Distance = round(R * c, 2)
                        distance_max = max(distance_max,Distance)

                        #### LOGGING STRING OUTPUT ####
                        airborne_str = airline + '|' + registration + '|' + aircraft + '|' + hexcode + '|'
                        new_set.append(airborne_str)
                        gone = list(set(current_set) - set(new_set))
                        new = list(set(new_set) - set(current_set))
                        temp_new = current_set.copy()

                        for add_el in new:
                            temp_new.append(add_el)
                        if temp_new != current_set:
                            for flight in range(0, len(temp_new)):
                                if not temp_new[flight] in current_set:
                                    if new_flight_status:
                                        print('<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>')
                                        print('NEW AIRCRAFT ALERT!!!')
                                    if military_status:
                                        print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                                        print('POSSIBLE MILITARY!!')
                                    if jumbo_status:
                                        print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
                                        print('JUMBO!!')
                                    if unknown_status:
                                        print('????????????????????????????????????????????????????????????????????????????????????????????????????????')
                                        print('WHAT ARE YOU!?')
                                    print(f"[{time.ctime()}] >>> {temp_new[flight]} Vector: [Alt: {altitude}ft | Airspeed: {speed}kts | Track: {track}deg | Vert Rate: {vert_rate}fpm | Distance {round(Distance,2)}km] ")
                                    fail_counter = 0


                        current_set = temp_new
                        temp_new = []

                        flight_register[hexcode] = {
                            "airline": airline,
                            "registration": registration,
                            "aircraft": aircraft,
                            "aircraft_thumbnail": aircraft_thumb,
                            "latitude": lat,
                            "longitude": lon,
                            "altitude": altitude,
                            "distance": Distance,
                            "first_seen_time": first_seen_time,
                        }

                    else:
                        #print(f'[{time.ctime()}] {hexcode} does not have fresh position data anymore')
                        pass

                    # Now let's update the aircraft dictionary with the new data
                    # only if the maximum distance is greater than the previous maximum

                    if hexcode in aircraft_dictionary:
                        #print(f'[{time.ctime()}] {hexcode} found in aircraft dictionary - only update if new distance is greater')
                        old_distance = aircraft_dictionary[hexcode]['distance']
                        if Distance > old_distance:
                            print(f'[{time.ctime()}] {hexcode} New distance {Distance}km is greater than previous distance {old_distance}km, update data!')
                            aircraft_dictionary[hexcode]['distance'] = Distance
                            aircraft_dictionary[hexcode]['airline'] = airline
                            aircraft_dictionary[hexcode]['registration'] = registration
                            aircraft_dictionary[hexcode]['aircraft'] = aircraft
                            aircraft_dictionary[hexcode]['aircraft_thumbnail'] = aircraft_thumb
                            aircraft_dictionary[hexcode]['latitude'] = lat
                            aircraft_dictionary[hexcode]['longitude'] = lon
                            aircraft_dictionary[hexcode]['altitude'] = altitude
                            aircraft_dictionary[hexcode]['first_seen_time'] = first_seen_time
                            with open(f"aircraft_dictionary.json", 'w') as file:
                                json.dump(aircraft_dictionary, file)
                                print(f'[{time.ctime()}] {hexcode} updated aircraft dictionary successfully!')
                    else:
                        print(f'[{time.ctime()}] {hexcode} not found in aircraft dictionary')
                        aircraft_dictionary.update(flight_register)
                        # Update the dictionary!
                        with open(f"aircraft_dictionary.json", 'w') as file:
                            json.dump(aircraft_dictionary, file)
                            print(f'[{time.ctime()}] {hexcode} updated aircraft dictionary successfully!')

        else:  # sleep for a few seconds before pinging again
            print(f'[{time.ctime()}] Nothing on radar ...')
            time.sleep(10)
            str_error = None
    except Exception as e:
        str_error = str(e)
        extra_info = str(traceback.format_exc())

        time_wait = 5
        fail_counter += 1
        err = open('errors.txt', 'a')
        err.write(f'[{time.ctime()}] <{hexcode}> {e} {extra_info} {fail_counter} times in a row\n')
        err.close()
        if fail_counter % 10 == 0:
            print(f'[{time.ctime()}] !!! Had an error!!!\n {e} {fail_counter} times in a row')

        #time.sleep(time_wait)
        continue
    if timer % 300 == 0:
        print(f"[{time.ctime()}] /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/")
    if timer % 3000 == 0:
        print('-')
        print('--')
        print('---')
        print(f"[{time.ctime()}] RESETTING CURRENT SET")
        print('---')
        print('--')
        print('-')

        current_set = []
        flying_hex = []
    # if air_count > 0:

    diagnostic_count+=1
    if diagnostic_count % 15 == 0:
        print(f"[{time.ctime()}]: Diagnostics Iteration {round(diagnostic_count,3)}: Max Distance: {round(distance_max,3)}km")
    timer += 1
    fail_counter = 0
    # add an array to pop in and out flights/hexes that are pingable