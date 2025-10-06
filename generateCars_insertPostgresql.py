import datetime as dt
from datetime import timedelta
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

import json
from datetime import datetime, timedelta, time
import random
import psycopg2 as db

date_format = "%d/%m/%Y %H:%M:%S"

import sys
import subprocess

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'faker'])
from faker import Faker


# generate new cars entry and exit

fake=Faker()

class CarPark:
    def __init__(self, Plate, LocationID, Entry_DateTime, Exit_DateTime, Parking_Charges):
        self.Plate = Plate
        self.LocationID = LocationID
        self.Entry_DateTime = Entry_DateTime
        self.Exit_DateTime = Exit_DateTime
        self.Parking_Charges = Parking_Charges

def generate_past_datetime_hours(now:datetime, hours:int) -> datetime:

    # Define the time range for the past 1 minutes
    end_date = now
    start_date = now - timedelta(hours=hours) 

    time_delta_total_seconds = int((end_date - start_date).total_seconds())

    # Generate a random number of seconds within the hour range
    random_seconds_past = random.randint(0, time_delta_total_seconds)
    random_date_base = start_date + timedelta(seconds=random_seconds_past)

    # Generate random time components between 9 AM and 8 PM
    random_hour = random.randint(9, 20)  # 9 to 20 (inclusive for 8 PM)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)

    # Combine date and time components
    generated_datetime = random_date_base.replace(
        hour=random_hour,
        minute=random_minute,
        second=random_second,
        microsecond=0  # Set microseconds to 0 for minute/second precision
    )

    return generated_datetime

# generate cars entering the carpark for the past 12 hours between 9 am to 8 pm for every minute and second
def createCarEntry():
    now = datetime.now()
    entry_time = generate_past_datetime_hours(now, 12) # Approximately 12 hours ago
    duration = entry_time - now
    charged = duration.seconds/60/60/2 * 60/100
    car = CarPark(
        Plate= fake.license_plate(),
        LocationID="Park"+str(random.randint(0, 5)),
        Entry_DateTime=entry_time.strftime(date_format),
        Exit_DateTime=now.strftime(date_format),
        Parking_Charges=charged
    )

    # return a json format
    return json.dumps(car.__dict__) 

def insertPostgresql():

    # Generate more cars, append to list and save csv
    carpark_system = []
    for i in range(10):
        thiscar_dict = eval(createCarEntry())
        carpark_system.append(list(thiscar_dict.values()))

    df = pd.DataFrame(carpark_system, columns=["Plate", "LocationID", "Entry_DateTime", "Exit_DateTime", "Parking_Charges"])
    df['Entry_DateTime'] = pd.to_datetime(df['Entry_DateTime'],format=date_format)
    df['Exit_DateTime'] = pd.to_datetime(df['Exit_DateTime'],format=date_format)
    data = [tuple(item) for index, item in df.iterrows()]
    data_for_db = tuple(data)


    # replace localhost with host.docker.internal
    conn_string="dbname='carpark_system' host='ec2-54-169-173-167.ap-southeast-1.compute.amazonaws.com' user='airflow' password='airflow'"

    conn=db.connect(conn_string)
    cur=conn.cursor()

    # insert multiple records in a single statement
    query = 'INSERT INTO public."CarPark"("Plate", "LocationID", "Entry_DateTime", "Exit_DateTime", "Parking_Charges") VALUES (%s, %s, %s, %s, %s)'
    # execute the query
    cur.executemany(query,data_for_db)

    # make it permanent by committing the transaction
    conn.commit()

    ## you can uncomment to check the data generated
    # df=pd.read_sql('select * from public."CarPark"',conn)
    # df.to_csv('/opt/airflow/dags/data/carpark_system.csv')

    print("-------Data Saved------")


default_args = {
    'owner': 'gmscher',
    'start_date': dt.datetime(2025, 7, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('carpark_system_generate_cars_DBdag',
         default_args=default_args,
         schedule=timedelta(minutes=5),      
                           # '0 * * * *',
         ) as dag:

    insertData = PythonOperator(task_id='GenerateCars',
                                python_callable=insertPostgresql)

insertData

