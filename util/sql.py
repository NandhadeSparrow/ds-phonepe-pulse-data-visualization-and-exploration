from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime,
    MetaData, BigInteger
    )
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
username = os.environ.get('sql_db_usr') 
password = os.environ.get('sql_db_pwd')
hostname = 'localhost'
port = os.environ.get('sql_db_endpoint') 
database = os.environ.get('sql_db_dbname')


# postgre
db_url = f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}'
# mysqlclient
# db_url = f'mysql+mysqldb://{username}:{password}@{hostname}/{database}'


# Create engine
engine = create_engine(db_url)


# Create a base class for your ORM models
Base = declarative_base()


# Define your ORM model
class Agg_User(Base):
    __tablename__ = 'agg_user'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    registered_users = Column(BigInteger)
    app_opens = Column(BigInteger)
    brand = Column(String)
    count = Column(BigInteger)
    percentage = Column(Integer)


class Agg_Trans(Base):
    __tablename__ = 'agg_users'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    transaction_name = Column(String)
    count = Column(BigInteger)
    amount = Column(BigInteger)


class Agg_Ins(Base):
    __tablename__ = 'agg_ins'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    transaction_name = Column(String)
    count = Column(BigInteger)
    amount = Column(BigInteger)


class Map_User(Base):
    __tablename__ = 'map_users'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    district = Column(String)
    registered_users = Column(BigInteger)
    app_opens = Column(BigInteger)



class Map_Trans(Base):
    __tablename__ = 'map_trans'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    district = Column(String)
    count = Column(BigInteger)
    amount = Column(BigInteger)

class Map_Ins(Base):
    __tablename__ = 'map_ins'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    district = Column(String)
    count = Column(BigInteger)
    amount = Column(BigInteger)


class Top_User(Base):
    __tablename__ = 'top_users'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    district = Column(String)
    registered_users = Column(BigInteger)


class Top_Trans(Base):
    __tablename__ = 'top_trans'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    district = Column(String)
    count = Column(BigInteger)
    amount = Column(BigInteger)


class Top_Ins(Base):
    __tablename__ = 'top_ins'
    _id = Column(Integer, primary_key=True, autoincrement=True)  # Fake primary key

    state = Column(String)
    year = Column(Integer)
    quarter = Column(Integer)
    district = Column(String)
    count = Column(BigInteger)
    amount = Column(BigInteger)


def sql():
    # Create the tables in the database
    Base.metadata.create_all(engine)

    # Create a session maker
    Session = sessionmaker(bind=engine)
    session = Session()
    return session