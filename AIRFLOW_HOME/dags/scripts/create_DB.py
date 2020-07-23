from datetime import date
import time
import random
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float,DateTime,Date
from sqlalchemy import select, desc,exists,and_
from sqlalchemy.sql import func
import os

def create_DB():
    pathDB = 'C:\\AIRFLOW_HOME\\DB'
    #Create DB and Tables 
    engine = create_engine('sqlite:///'+pathDB+'\\superpharmDB.db', echo=False,connect_args={'timeout': 100})
    metadata = MetaData()

    items = Table('items', metadata,
        Column('id', Integer,primary_key=True),
        Column('name', Integer),
        Column('description', String),
        Column('department', String),
        Column('category', String),
        Column('brand', String),
        Column('url', String),
        Column('image_url', String),
        Column('update_date', DateTime)
    )
    
    prices = Table('prices', metadata,
        Column('item_id', Integer),
        Column('amount', Float),
        Column('date', Date)
    )
    
    #Create Tables
    metadata.create_all(engine)

if __name__ == "__main__":
    create_DB()
    
