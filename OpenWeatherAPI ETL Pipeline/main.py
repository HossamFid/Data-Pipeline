
from OpenWeatherETL import weather_ETL, current_datetime

def date_now():
    current_time = current_datetime()
    print(f"The current datetime is: {current_time}")

if __name__ == "__main__":
    current_datetime()
    weather_ETL()