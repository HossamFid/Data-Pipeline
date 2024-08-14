import json 
import pandas as pd 
import requests
import datetime
import time
import os 
from cities import cities 


# https://api.openweathermap.org/data/2.5/weather?q=London&appid={API key}

# https://api.openweathermap.org/data/2.5/weather?q=cairo&appid=c0810f4a8783c59ca8ccd3ce01949653

# resources and tools 
# https://openweathermap.org/api/one-call-3
# https://jsonformatter.org/


# handle credentials 
# def creds_init():
#     with open(f'creds.txt', 'r') as f:
#         content = f.read()
#         return content
    
api_key = 'c0810f4a8783c59ca8ccd3ce01949653'

# fahr temperature converter
def kalvin_fahr(kelvin):
    fahr =  ((kelvin - 273.15) * 1.8) + 32
    return round(fahr,2) 

#  Unix to UTC TimeZone converter
def utc_converter(unix_time):
    #Convert Unix timestamp to a UTC 
    #utc_time = datetime.datetime.utcfromtimestamp(unix_time) # may eventually be deprecated 
    utc_time = datetime.datetime.fromtimestamp(unix_time, tz=datetime.timezone.utc) 

    cairo_offset = datetime.timedelta(hours=2)
    cairo_time = utc_time + cairo_offset

    return cairo_time

# current datetime to name the CSV files 
def current_datetime():
    now = datetime.datetime.now()
    date_time = now.strftime("%Y-%m-%d_%H-%M-%S") # _ for filesystems renaming  
    #print(formatted_now)
    return date_time



def weather_ETL():
    # elements storage 
    data_dict = {}

    total_iterations = len(cities)
    #print(f'total: {total_iterations}')

    for index, city in enumerate(cities):

        response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}")

        if response.status_code == 200:
            #print(response.text)

            data = response.json()

        

            # extract data entires 
            lon = data['coord']['lon']
            lat = data['coord']['lat']

            weather_id = data['weather'][0]['id']
            weather_main = data['weather'][0]['main']
            weather_description = data['weather'][0]['description']
            weather_icon = data['weather'][0]['icon']

            base = data['base']

            temp = kalvin_fahr(data['main']['temp'])
            feels_like = kalvin_fahr(data['main']['feels_like'])
            temp_min = kalvin_fahr(data['main']['temp_min'])
            temp_max = kalvin_fahr(data['main']['temp_max'])
            pressure = data['main']['pressure']
            humidity = data['main']['humidity']
            sea_level = data['main']['sea_level']
            grnd_level = data['main']['grnd_level']

            visibility = data['visibility']

            wind_speed = data['wind']['speed']
            wind_deg = data['wind']['deg']

            clouds_all = data['clouds']['all']



            try: 
                sys_type = data['sys']['type']
                sys_id = data['sys']['id']
            except KeyError as e:
                sys_type = None
                sys_id = None

            country = data['sys']['country']

            sunrise = utc_converter(data['sys']['sunrise'] + data['timezone'])
            sunset = utc_converter(data['sys']['sunset'] + data['timezone'])
            record_time = utc_converter(data['dt'] + data['timezone'])
            
            id = data['id']
            city_name = data['name']
            cod = data['cod']


            #print(f"This is {city_name} weather data, {country}")
            #print(f"Temperature is {temp}K,and it feels like: {feels_like}K")

            # store data into dictioanry 
            data_dict[city_name] = {
            "lon": lon, "lat": lat,
            "weather_id": weather_id, "weather_main": weather_main,
            "weather_description": weather_description, "weather_icon": weather_icon,
            "base": base, "temp": temp,
            "feels_like": feels_like, "temp_min": temp_min,
            "temp_max": temp_max, "pressure": pressure,
            "humidity": humidity, "sea_level": sea_level,
            "grnd_level": grnd_level, "visibility": visibility,
            "wind_speed": wind_speed, "wind_deg": wind_deg,
            "clouds_all": clouds_all, "sys_type": sys_type,
            "sys_id": sys_id, "city": city_name,
            "country": country, "sunrise": sunrise,
            "sunset": sunset, "record_time": record_time,
            "city_id": id, "cod": cod
        }
        else:
        
            print(f"Failed to retrieve data. Status code: {response.status_code}")
        
            if response.status_code == 404:
                print("city not found.")

        print(f'Running {index+1} of {total_iterations}, and remaining runs are {total_iterations - (index + 1)}')
        time.sleep(1)

    #print(data_dict)

    # create DataFrame and check file exists 
    current = current_datetime()
    csv_filename= f'open_weather_data_{current}.csv'
    if os.path.exists(csv_filename):
        print('file existed ......')
    else:
        data_list = list(data_dict.values())
        df = pd.DataFrame(data_list)
        #print(df)
        df.to_csv(f'Data Files/OW_data_{current}.csv', index=False)
        print(f'Data file, {csv_filename} has been created succesfully ..................')

#weather_ETL()