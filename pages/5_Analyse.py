import streamlit as st
import pandas as pd
import altair as alt
import streamlit as st
from util import yt_yt as yt
from util.yt_sql import sql, YtChannelModel, YtVideosModel, YtCommentsModel
from util import yt_yt as yt
from util import yt_mdb
from util.dataframe import df_with_link

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


channels = pd.DataFrame(coll_channels.find({},{"_id": 0}))
videos = pd.DataFrame(coll_videos.find({},{"_id": 0}))
comments = pd.DataFrame(coll_comments.find({},{"_id": 0}))
# channels.reset_index(inplace=True)
# videos.reset_index(inplace=True)
# comments.reset_index(inplace=True)
catergories = {value: key for key, value in youtube.getCategories().items()}


st.markdown("# Analyse SQL Data")


st.markdown("### Views of the last 50 videos")

def create_chart(df):
    chart = alt.Chart(df).mark_line().encode(
        x='published:T',
        y='view_count:Q',
        color='channel_name:N',
        tooltip=['index:T', 'view_count:Q']
    ).properties(
        width=800,
        height=400
    ).interactive()
    return chart
videos['channel_name'] = videos['channel_id'].map(channels.set_index('channel_id')['channel_name'])
st.altair_chart(create_chart(videos), use_container_width=True)


st.markdown("### Filter catagory of videos in each channel")

selected_option = st.selectbox(
    'Select a category:',
    catergories
)
category_id = catergories[selected_option]

# Display the selected option
st.write('Category:', selected_option)

df_videos = pd.DataFrame(videos)
df_categories = df_videos[df_videos['category_id'] == category_id]

if df_categories.shape[0] == 0:
    st.warning('No videos available for this category')
else:
    df_count = df_categories[
        ['channel_name', 'category_id']
        ].groupby('channel_name').count().sort_values(
        ['category_id'], ascending=False).rename(
            columns={
                'category_id': 'Video Count',
                }
            ).reset_index()

    st.write('Sorted by video count in descending order')
    st.write(df_count)

    for i in df_count['channel_name'].to_list():
        with st.expander(i):
            df_vids = df_categories[['title', 'video_id']][df_categories['channel_name'] == i]
            df_vids['video_id'] = 'https://www.youtube.com/watch?v='+df_vids['video_id']
            df_vids = df_vids.rename(columns={'title': 'Videos'}).reset_index(drop=True)
            df_with_link(df_vids, 'Videos', 'video_id')