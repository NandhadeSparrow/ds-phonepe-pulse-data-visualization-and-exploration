from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime,
    MetaData
    )
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
aws_db_usr = os.environ.get('aws_db_usr') 
aws_db_pwd = os.environ.get('aws_db_pwd')
aws_db_endpoint = os.environ.get('aws_db_endpoint') 
aws_db_dbname = os.environ.get('aws_db_dbname')

db_channels_coll_name = os.environ.get('db_channels_coll_name')
db_videos_coll_name = os.environ.get('db_videos_coll_name')
db_comments_coll_name = os.environ.get('db_comments_coll_name')


# SQLAlchemy database URL format for connecting to AWS RDS PostgreSQL
db_url = f'postgresql://{aws_db_usr}:{aws_db_pwd}@{aws_db_endpoint}/{aws_db_dbname}'

# Create engine
engine = create_engine(db_url)


# Create a base class for your ORM models
Base = declarative_base()


# Define your ORM model
class YtChannelModel(Base):
    __tablename__ = db_channels_coll_name

    channel_id = Column(String, primary_key=True)
    channel_name = Column(String)
    channel_desc = Column(String)
    username = Column(String)
    published = Column(DateTime)
    thumbnail = Column(String)
    country = Column(String)
    videos_id = Column(String)
    view_count = Column(Integer)
    sub_count = Column(Integer)
    vid_count = Column(Integer)

class YtVideosModel(Base):
    __tablename__ = db_videos_coll_name

    video_id = Column(String, primary_key=True)
    published = Column(DateTime)
    channel_id = Column(String)
    title = Column(String)
    thumbnail = Column(String)
    category_id = Column(String)
    duration = Column(String)
    view_count = Column(Integer)
    like_count = Column(Integer)
    comment_count = Column(Integer)


class YtCommentsModel(Base):
    __tablename__ = db_comments_coll_name

    comment_id = Column(String, primary_key=True)
    channel_id = Column(String)
    video_id = Column(String)
    text_display = Column(String)


def sql():


    # Create the tables in the database
    Base.metadata.create_all(engine)

    # Create a session maker
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

