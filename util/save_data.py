import pandas as pd
import json
import mysql.connector


import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
aws_db_usr = os.environ.get('aws_db_usr') 
aws_db_pwd = os.environ.get('aws_db_pwd')
aws_db_endpoint = os.environ.get('aws_db_endpoint') 
aws_db_dbname = os.environ.get('aws_db_dbname')


agg_trans_states = 'pulse/data/aggregated/transaction/country/india/state/'
agg_users_states = 'pulse/data/aggregated/user/country/india/state/'
map_trans_states = 'pulse/data/map/transaction/hover/country/india/state/'
map_users_states = 'pulse/data/map/user/country/hover/india/state/'
top_trans_states = 'pulse/data/top/transaction/country/india/state/'
top_users_states = 'pulse/data/top/user/country/india/state/'


def get_items(states):
    data = []
    for state in os.listdir(states):
        years = states+state+'/'
        for year in os.listdir(years):
            quarters = years+year+'/'
            for file in os.listdir(quarters):
                file_path = quarters+file
                with open(file_path) as json_file:
                    file_data = json.load(json_file)
                    file_data['State'] = state
                    file_data['Year'] = year
                    file_data['Quarter'] = int(file.strip('.json'))
                    data.append(file_data)
    return data


def save_agg_trans():
    agg_trans = []
    items = get_items(agg_trans_states)
    for data in items:
        for payments in data['data']['transactionData']:
            agg_trans.append({
                'State':data['State'],
                'Year': data['Year'],
                'Quarter':data['Quarter'],
                'Transaction_Name':payments['name'],
                'Count':payments['paymentInstruments'][0]['count'],
                'Amount':payments['paymentInstruments'][0]['amount'],
            })
    return pd.DataFrame(agg_trans)


def save_agg_users():
    agg_users = []
    items = get_items(agg_users_states)
    for data in items:
        print(data)
        for brand_data in data['data']['usersByDevice']:
            agg_users.append({
                'State': data['State'],
                'Year': data['Year'],
                'Quarter': data['Quarter'],
                'Registered_Users':data['data']['aggregated']['registeredUsers'],
                'App_Opens':data['data']['aggregated']['appOpens'],
                'Brand':brand_data['brand'],
                'Count':brand_data['count'],
                'Percentage':brand_data['percentage'],
            })
    return pd.DataFrame(agg_users)


def save_map_trans():
    map_trans = []
    items = get_items(map_trans_states)
    for data in items:
        for entry in data['data']['hoverDataList']:
            map_trans.append({
                'State': data['State'],
                'Year': data['Year'],
                'Quarter': data['Quarter'],
                'District':entry['name'],
                'Count':entry['metric'][0]['count'],
                'Amount':entry['metric'][0]['amount'],
            })
    return pd.DataFrame(map_trans)


def save_map_users():
    map_users = []
    items = get_items(map_users_states)
    for data in items:
        for district, values in data["data"]["hoverData"].items():
            map_users.append({
                'State': data['State'],
                'Year': data['Year'],
                'Quarter': data['Quarter'],
                "District": district if district else 'NA',
                "Registered_Users": values["registeredUsers"],
                "App_Opens": values["appOpens"],
            })
    return pd.DataFrame(map_users)


def save_top_trans():
    top_trans_districts = []
    top_trans_pincodes = []
    items = get_items(top_trans_states)
    for data in items:
        for district in data['data']['districts']:
            top_trans_districts.append({
                'State': data['State'],
                'Year': data['Year'],
                'Quarter': data['Quarter'],
                'District':district['entityName'],
                'Count':district['metric']['count'],
                'Amount':district['metric']['amount'],
            })
    
        for pincode in data['data']['pincodes']:
            top_trans_pincodes.append({
                'State': data['State'],
                'Year': data['Year'],
                'Quarter': data['Quarter'],
                'PIN':pincode['entityName'],
                'Count':pincode['metric']['count'],
                'Amount':pincode['metric']['amount'],
                
            })
    top_trans_districts_df = pd.DataFrame(top_trans_districts)
    top_trans_pincodes_df = pd.DataFrame(top_trans_pincodes)
    return top_trans_districts_df,top_trans_pincodes_df


def save_top_users():
    top_user_districts = []
    top_user_pincodes = []
    items = get_items(top_users_states)
    for data in items:
        for district in data['data']['districts']:
            top_user_districts.append({ 
                'State': data['State'],
                'Year': data['Year'],
                'Quarter': data['Quarter'],
                'District':district['name'],
                'Registered_Users':district['registeredUsers'],
            })
        for pincode in data['data']['pincodes']:
            top_user_pincodes.append({ 
                'State': data['State'],
                'Year': data['Year'],
                'Quarter': data['Quarter'],
                'PIN':pincode['name'],
                'Registered_Users':pincode['registeredUsers'],
            })
    top_user_district_df = pd.DataFrame(top_user_districts)
    top_user_pincodes_df = pd.DataFrame(top_user_pincodes)
    return top_user_district_df,top_user_pincodes_df


def save_data():
    # cnx = mysql.connector.connect(
    #     user=aws_db_usr,
    #     password=aws_db_pwd,
    #     host=aws_db_endpoint,
    #     database=aws_db_dbname
    # )
    
    print(save_agg_trans())
    print(save_agg_users())
    print(save_map_trans())
    print(save_map_users())
    print(save_top_trans())
    print(save_top_users())

    # cnx.close()

    
save_data()