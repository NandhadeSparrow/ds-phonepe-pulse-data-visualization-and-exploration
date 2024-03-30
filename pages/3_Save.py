import streamlit as st
from util import yt_yt as yt
from util import yt_mdb
from util.yt_sql import sql, YtChannelModel, YtVideosModel, YtCommentsModel
from pandas import DataFrame

import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
mdb_dbName = os.environ.get('mdb_dbName')
mdb_usr = os.environ.get('mdb_usr')
mdb_pwd = os.environ.get('mdb_pwd')
mdb_appName = os.environ.get('mdb_appName')

session = sql()
youtube = yt.yt()
mdb = yt_mdb.mdb()

coll_channels = mdb.get_coll_channels()
coll_videos = mdb.get_coll_videos()
coll_comments = mdb.get_coll_comments()


channels = list(coll_channels.find({},{"_id": 0}))
videos = list(coll_videos.find({},{"_id": 0}))
comments = list(coll_comments.find({},{"_id": 0}))


def reset_sql():
    session.query(YtChannelModel).delete()
    session.query(YtVideosModel).delete()
    session.query(YtCommentsModel).delete()


def save(selected_channels, selected_videos, selected_comments):
    channels = [YtChannelModel(**i) for i in selected_channels]
    videos = [YtVideosModel(**i) for i in selected_videos]
    comments = [YtCommentsModel(**i) for i in selected_comments]

    session.add_all(channels)
    session.add_all(videos)
    session.add_all(comments)
    session.commit()
    entries = session.query(YtChannelModel).all()
    df = DataFrame([i.__dict__ for i in entries]).drop('_sa_instance_state', axis=1)
    st.markdown('## From SQL DB')
    st.markdown('### Channels')
    st.write(df)
    entries = session.query(YtVideosModel).all()
    df = DataFrame([i.__dict__ for i in entries]).drop('_sa_instance_state', axis=1)
    st.markdown('### Videos')
    st.write(df)
    entries = session.query(YtCommentsModel).all()
    df = DataFrame([i.__dict__ for i in entries]).drop('_sa_instance_state', axis=1)
    st.markdown('### Comments')
    st.write(df)


selected_ch = st.multiselect(
    label = "Choose channels", 
    options = [ch['channel_name'] for ch in channels], 
)

if not selected_ch:
    st.error("Please select at least one channel.")
else:
    selected_channel_ids = [ch['channel_id'] for ch in channels if ch['channel_name'] in selected_ch]
    selected_channels = [ch for ch in channels if ch['channel_id'] in selected_channel_ids]
    selected_videos = [v for v in videos if v['channel_id'] in selected_channel_ids]
    selected_comments = [c for c in comments if c['channel_id'] in selected_channel_ids]
    

    st.markdown('## Selected channels data')
    st.markdown('### Channels')
    st.write(DataFrame(selected_channels))
    st.markdown('### Videos')
    st.write(DataFrame(selected_videos))
    st.markdown('### Comments')
    st.write(DataFrame(selected_comments))

    st.write('This button copies data from MongoDB to AWS RDS PostgreSQL Database')
    if st.button('Save in SQL Database'):    
        save(selected_channels, selected_videos, selected_comments)
        st.success("All entries have been saved in the tables.")

    st.write('This button deletes all entries from all tables in AWS RDS PostgreSQL Database')
    if st.button("Delete All Entries in SQL Database"):
        reset_sql()
        st.success("All entries have been deleted from the tables.")



