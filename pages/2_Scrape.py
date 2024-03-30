import streamlit as st
import matplotlib.pyplot as plt
from pandas import DataFrame
from util import yt_yt as yt
from util import yt_mdb
from util.dataframe import df_with_link

youtube = yt.yt()
mdb = yt_mdb.mdb()
coll_channels = mdb.get_coll_channels()
coll_videos = mdb.get_coll_videos()
coll_comments = mdb.get_coll_comments()
chIds = []

def updateEntries():
    global chIds
    channels = coll_channels.find({},{"_id": 0})
    dfChannels = DataFrame(channels)
    st.session_state['dfChannels'] = dfChannels
    if st.session_state['dfChannels'].shape[0] > 0:
        chIds = dfChannels['channel_id'].tolist()
updateEntries()


def make_clickable(url):
    return f'<a target="_blank" href="{url}">{url}</a>'


def plot(df):
    import streamlit as st
    st.markdown('### Performance of latest 50 videos')
    st.line_chart(df[['view_count', 'like_count', 'comment_count']])


def scrape():
    chId = st.text_input('Channel ID')
    if chId in chIds: 
        st.error('Channel data aready exists. Give different channel ID')
    st.write('On clicking the below button the data related to this channel ID is requested from Youtube Data API.')
    if st.button('Get Details') and chId and chId not in chIds:
        chDetails = youtube.getChDetails(chId)
        if chDetails == -1:
            st.error("Can't get channel details. Check channel ID.")
        else:
            # get data from youtube api
            videos = youtube.getVideos(chDetails['videos_id'])
            ytComments = [] 
            progress_bar = st.sidebar.progress(0)
            status_text = st.sidebar.empty()
            for j in range(len(videos)):
                i = videos[j]
                res = youtube.getComments(i['contentDetails']['videoId'])
                ytComments.extend(res)
                progress_bar.progress((j+1)*2)
                status_text.text(f'{(j+1)*2} % Complete')
            progress_bar.empty()
            status_text.text('Scraping completed')
            

            # displaying
            st.markdown('### Latest videos')
            dfVids = DataFrame(videos)

            dfVids['url'] = 'https://www.youtube.com/watch?v='+dfVids['video_id']
            df_with_link(dfVids, 'title', 'url')

            plot(dfVids)

            catNames = youtube.getCategories()
            fig, ax = plt.subplots()
            slice_sizes = (dfVids['category_id'].value_counts() / dfVids.shape[0]) * 100
            slice_sizes = slice_sizes.reset_index()
            labels = [catNames[i] for i in slice_sizes['category_id']]
            ax.pie(slice_sizes['count'], 
                   labels=labels, 
                   startangle=90)
            ax.axis('equal')
            st.markdown('### Content category:')
            st.pyplot(fig)


            # saving in mongodb
            coll_channels.insert_one(chDetails)
            coll_videos.insert_many(videos)
            coll_comments.insert_many(ytComments)


            # update entries
            updateEntries()         

st.write("# Scrape Youtube Channels Data")

scrape()

if st.session_state['dfChannels'].shape[0] > 0:
    st.markdown('### Channels in MongoDB Atlas:')
    st.write(st.session_state['dfChannels'])

if st.session_state['dfChannels'].shape[0] > 0 and st.button('Delete all data in MongoDB Atlas'):
    coll_channels.delete_many({})
    coll_videos.delete_many({})
    coll_comments.delete_many({})

    st.session_state['dfChannels'] = None
    st.rerun()