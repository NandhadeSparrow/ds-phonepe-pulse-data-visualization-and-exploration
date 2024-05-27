import streamlit as st
import pandas as pd
import streamlit as st
from util.sql import sql, Agg_User, Agg_Trans, Agg_Ins, Map_User, Map_Trans, Map_Ins, Top_User, Top_Trans, Top_Ins
import os
from dotenv import load_dotenv, find_dotenv
import plotly.express as px
import json
import requests


load_dotenv(find_dotenv())
mdb_dbName = os.environ.get('mdb_dbName')
mdb_usr = os.environ.get('mdb_usr')
mdb_pwd = os.environ.get('mdb_pwd')
mdb_appName = os.environ.get('mdb_appName')

session = sql()


agg_users = pd.DataFrame([i.__dict__ for i in session.query(Agg_User).all()])
agg_trans = pd.DataFrame([i.__dict__ for i in session.query(Agg_Trans).all()])
agg_ins = pd.DataFrame([i.__dict__ for i in session.query(Agg_Ins).all()])
map_users = pd.DataFrame([i.__dict__ for i in session.query(Map_User).all()])
map_trans = pd.DataFrame([i.__dict__ for i in session.query(Map_Trans).all()])
map_ins = pd.DataFrame([i.__dict__ for i in session.query(Map_Ins).all()])
top_users = pd.DataFrame([i.__dict__ for i in session.query(Top_User).all()])
top_trans = pd.DataFrame([i.__dict__ for i in session.query(Top_Trans).all()])
top_ins = pd.DataFrame([i.__dict__ for i in session.query(Top_Ins).all()])

# channels.reset_index(inplace=True)
# videos.reset_index(inplace=True)
# comments.reset_index(inplace=True)

map_sum_users =  map_users.groupby(['year', 'quarter', 'state'])[['registered_users']].sum().reset_index()
map_sum_trans =  map_trans.groupby(['year', 'quarter', 'state'])[['count', 'amount']].sum().reset_index().rename(columns={'count':'trans_count', 'amount': 'trans_amount'})
map_sum_ins =  map_ins.groupby(['year', 'quarter', 'state'])[['count', 'amount']].sum().reset_index().rename(columns={'count':'ins_count', 'amount': 'ins_amount'})

agg_sum_users =  agg_users.groupby(['year', 'quarter', 'state', 'brand'])[['count']].sum().reset_index()
agg_sum_trans =  agg_trans.groupby(['year', 'quarter', 'state', 'transaction_name'])[['count', 'amount']].sum().reset_index().rename(columns={'count':'trans_count', 'amount': 'trans_amount'})
agg_sum_ins =  agg_ins.groupby(['year', 'quarter', 'state'])[['count', 'amount']].sum().reset_index().rename(columns={'count':'ins_count', 'amount': 'ins_amount'})

top_sum_users =  top_users.groupby(['year', 'quarter', 'state'])[['registered_users']].sum().reset_index()
top_sum_trans =  top_trans.groupby(['year', 'quarter', 'state'])[['count', 'amount']].sum().reset_index().rename(columns={'count':'trans_count', 'amount': 'trans_amount'})
top_sum_ins =  top_ins.groupby(['year', 'quarter', 'state'])[['count', 'amount']].sum().reset_index().rename(columns={'count':'ins_count', 'amount': 'ins_amount'})


map_sum = map_sum_users.merge(map_sum_trans, on=['year', 'quarter', 'state'], how='inner')
map_sum = map_sum.merge(map_sum_ins, on=['year', 'quarter', 'state'], how='inner')
# st.write(map_sum)

# st.write(agg_sum_users)
# st.write(agg_sum_trans)
# st.write(agg_sum_ins)

top_sum = top_sum_users.merge(top_sum_trans, on=['year', 'quarter', 'state'], how='inner')
top_sum = top_sum.merge(top_sum_ins, on=['year', 'quarter', 'state'], how='inner')
# st.write(map_sum)

st.markdown("## Maps")


# if quarter not in map_sum[map_sum['year'] == year]['quarter'].unique():
#     quarter = 'All'
# if year not in map_sum[map_sum['quarter'] == quarter]['year'].unique():
#     year = 'All'

def map(title, df, column):
    # st.write(df)
    df["state"] = df["state"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    df["state"] = df["state"].str.replace("-"," ")
    df["state"] = df["state"].str.title()
    df["state"] = df["state"].str.replace("dadra & nagar haveli & daman & diu","Dadra and nagar haveli and Daman and Diu")
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    geojson = json.loads(response.content)
    state_names = []
    for feature in geojson["features"]:
        state_names.append((feature['properties']['ST_NM']))
    state_names.sort()

    fig_india = px.choropleth(df, geojson=geojson, locations="state", 
                                featureidkey="properties.ST_NM",
                                color = column, color_continuous_scale = "Spectral",
                                range_color = (df[column].min(), df[column].max()), # Based upon the values present in the dataframe the color range varies
                                hover_name = "state", 
                                hover_data=df.columns, 
                                title = title, fitbounds = "locations",
                                # height = 650, width = 600
                                )
    fig_india.update_geos(visible = False)  # to remove background world lines present around india
    st.plotly_chart(fig_india)

def graph(key, df, title, column):
    col1, col2 = st.columns([1, 3])

    with col1:
        state = st.selectbox("Select State", ['All', *df['state'].unique()], key=key+'graph')
        if state != 'All':
            df = df[df['state']==state]
    with col2:
        df['yq'] = df['year']+df['quarter']/4
        fig = px.bar(df, x="yq", y=column,
                title=title)
        st.plotly_chart(fig)


def get_map_df(df, filtered):
    sum_cols = df.drop(['year', 'quarter', 'state'], axis=1).columns
    for i in filtered:
        if filtered[i] != 'All':
            df = df[df[i] == filtered[i]]
    return df.groupby(['state'])[sum_cols].sum().reset_index()
   

def section_map(key, df, title, column):

    st.markdown('### '+title)
    col1, col2 = st.columns([1, 3])

    with col1:
        column = st.selectbox("Select Value for Color", df.drop(['year', 'quarter', 'state'], axis=1).select_dtypes(include='number').columns, key=key+'c')
        filtered = {}
        for i in ['year', 'quarter', *df.select_dtypes(exclude='number').columns]:
            filtered[i] = st.selectbox(f"Select {i}", ['All', *df[i].unique()], key=key+i)
            
    with col2:
        map(title, get_map_df(df, filtered), column)


maps = [
    {
        'df': map_sum,
        'title': 'Summary',
        'column': 'registered_users'
    },
    {
        'df': agg_sum_users,
        'title': 'Aggregated Users',
        'column': 'count'
    },
    {
        'df': agg_sum_trans,
        'title': 'Aggregated Transaction',
        'column': 'trans_amount'
    },
    {
        'df': agg_sum_ins,
        'title': 'Aggregated Insurance',
        'column': 'ins_amount'
    },
    {
        'df': top_sum_users,
        'title': 'Top Users',
        'column': 'registered_users'
    },
    {
        'df': top_sum_trans,
        'title': 'Top Transactions',
        'column': 'trans_amount'
    },
    {
        'df': top_sum_ins,
        'title': 'Top Insurance Amounts',
        'column': 'ins_amount'
    },
]

for i in range(len(maps)):
    section_map(f'selectbox_{i}', maps[i]['df'], maps[i]['title'], maps[i]['column'])
    graph(f'selectbox_{i}', maps[i]['df'], maps[i]['title'], maps[i]['column'])