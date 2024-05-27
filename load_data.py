import pandas as pd
import json
from util.sql import sql, Agg_User, Agg_Trans, Agg_Ins, Map_User, Map_Trans, Map_Ins, Top_User, Top_Trans, Top_Ins

import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

sql_db_usr = os.environ.get('sql_db_usr') 
sql_db_pwd = os.environ.get('sql_db_pwd')
sql_db_endpoint = os.environ.get('sql_db_endpoint') 
sql_db_dbname = os.environ.get('sql_db_dbname')


session = sql()


agg_trans_states = 'data/aggregated/transaction/country/india/state/'
agg_users_states = 'data/aggregated/user/country/india/state/'
agg_insurance_states = 'data/aggregated/insurance/country/india/state/'

map_trans_states = 'data/map/transaction/hover/country/india/state/'
map_users_states = 'data/map/user/hover/country/india/state/'
map_insurance_states = 'data/map/insurance/hover/country/india/state/'

top_trans_states = 'data/top/transaction/country/india/state/'
top_users_states = 'data/top/user/country/india/state/'
top_insurance_states = 'data/top/insurance/country/india/state/'


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


def save_agg_users():
    agg_users = []
    items = get_items(agg_users_states)
    for data in items:
        if data['data']['usersByDevice']:
            for brand_data in data['data']['usersByDevice']:
                agg_users.append({
                    'state': data['State'],
                    'year': data['Year'],
                    'quarter': data['Quarter'],
                    'registered_users':data['data']['aggregated']['registeredUsers'],
                    'app_opens':data['data']['aggregated']['appOpens'],
                    'brand':brand_data['brand'],
                    'count':brand_data['count'],
                    'percentage':brand_data['percentage'],
                })
    return pd.DataFrame(agg_users)


def save_agg_trans():
    agg_trans = []
    items = get_items(agg_trans_states)
    for data in items:
        for payments in data['data']['transactionData']:
            agg_trans.append({
                'state':data['State'],
                'year': data['Year'],
                'quarter':data['Quarter'],
                'transaction_name':payments['name'],
                'count':payments['paymentInstruments'][0]['count'],
                'amount':payments['paymentInstruments'][0]['amount'],
            })
    return pd.DataFrame(agg_trans)


def save_agg_ins():
    agg_trans = []
    items = get_items(agg_insurance_states)
    for data in items:
        for ins in data['data']['transactionData']:
            agg_trans.append({
                'state':data['State'],
                'year': data['Year'],
                'quarter':data['Quarter'],
                'transaction_name':ins['name'],
                'count':ins['paymentInstruments'][0]['count'],
                'amount':ins['paymentInstruments'][0]['amount'],
            })
    return pd.DataFrame(agg_trans)


def save_map_users():
    map_users = []
    items = get_items(map_users_states)
    for data in items:
        for district, values in data["data"]["hoverData"].items():
            map_users.append({
                'state': data['State'],
                'year': data['Year'],
                'quarter': data['Quarter'],
                "district": district if district else 'NA',
                "registered_users": values["registeredUsers"],
                "app_opens": values["appOpens"],
            })
    return pd.DataFrame(map_users)


def save_map_trans():
    map_trans = []
    items = get_items(map_trans_states)
    for data in items:
        for entry in data['data']['hoverDataList']:
            map_trans.append({
                'state': data['State'],
                'year': data['Year'],
                'quarter': data['Quarter'],
                'district':entry['name'],
                'count':entry['metric'][0]['count'],
                'amount':entry['metric'][0]['amount'],
            })
    return pd.DataFrame(map_trans)


def save_map_ins():
    map_trans = []
    items = get_items(map_insurance_states)
    for data in items:
        for entry in data['data']['hoverDataList']:
            map_trans.append({
                'state': data['State'],
                'year': data['Year'],
                'quarter': data['Quarter'],
                'district':entry['name'],
                'count':entry['metric'][0]['count'],
                'amount':entry['metric'][0]['amount'],
            })
    return pd.DataFrame(map_trans)


def save_top_users():
    top_user_districts = []
    top_user_pincodes = []
    items = get_items(top_users_states)
    for data in items:
        for district in data['data']['districts']:
            top_user_districts.append({ 
                'state': data['State'],
                'year': data['Year'],
                'quarter': data['Quarter'],
                'district':district['name'],
                'registered_users':district['registeredUsers'],
            })
        # for pincode in data['data']['pincodes']:
        #     top_user_pincodes.append({ 
        #         'State': data['State'],
        #         'Year': data['Year'],
        #         'Quarter': data['Quarter'],
        #         'PIN':pincode['name'],
        #         'Registered_Users':pincode['registeredUsers'],
        #     })
    top_user_district_df = pd.DataFrame(top_user_districts)
    # top_user_pincodes_df = pd.DataFrame(top_user_pincodes)
    return top_user_district_df


def save_top_trans():
    top_trans_districts = []
    top_trans_pincodes = []
    items = get_items(top_trans_states)
    for data in items:
        item = {}
        for district in data['data']['districts']:
            top_trans_districts.append({
                'state': data['State'],
                'year': data['Year'],
                'quarter': data['Quarter'],
                'district':district['entityName'],
                'count':district['metric']['count'],
                'amount':district['metric']['amount'],
            })
        # for pincode in data['data']['pincodes']:
        #     top_trans_pincodes.append({
        #         'State': data['State'],
        #         'Year': data['Year'],
        #         'Quarter': data['Quarter'],
        #         'PIN':pincode['entityName'],
        #         'Count':pincode['metric']['count'],
        #         'Amount':pincode['metric']['amount'],
                
        #     })
    top_trans_districts_df = pd.DataFrame(top_trans_districts)
    # top_trans_pincodes_df = pd.DataFrame(top_trans_pincodes)
    return top_trans_districts_df


def save_top_ins():
    top_trans_districts = []
    top_trans_pincodes = []
    items = get_items(top_insurance_states)
    for data in items:
        for district in data['data']['districts']:
            top_trans_districts.append({
                'state': data['State'],
                'year': data['Year'],
                'quarter': data['Quarter'],
                'district':district['entityName'],
                'count':district['metric']['count'],
                'amount':district['metric']['amount'],
            })
    
        # for pincode in data['data']['pincodes']:
        #     top_trans_pincodes.append({
        #         'State': data['State'],
        #         'Year': data['Year'],
        #         'Quarter': data['Quarter'],
        #         'PIN':pincode['entityName'],
        #         'Count':pincode['metric']['count'],
        #         'Amount':pincode['metric']['amount'],
                
        #     })
    top_trans_districts_df = pd.DataFrame(top_trans_districts)
    # top_trans_pincodes_df = pd.DataFrame(top_trans_pincodes)
    return top_trans_districts_df


if __name__ == '__main__':
    models = [Agg_User, Agg_Trans, Agg_Ins, Map_User, Map_Trans, Map_Ins, Top_User, Top_Trans, Top_Ins]

    for i in models:
        session.query(i).delete()

    # save_funcs = [
    #     (save_agg_users, Agg_User),
    #     (save_agg_trans, Agg_Trans),
    #     (save_agg_ins, Agg_Ins),
    #     (save_map_users, Map_User),
    #     (save_map_trans, Map_Trans),
    #     (save_map_ins, Map_Ins),
    #     (save_top_users, Top_User),
    #     (save_top_trans, Top_Trans),
    #     (save_top_ins, Top_Ins),
    # ]

    # for i in save_funcs:
    #     entries = i[0]()
    #     print(entries)
    #     model = i[1]
    #     data = [model(**entry.to_dict()) for i, entry in entries.iterrows()]
    #     session.add_all(data)
    data = [Agg_User(**entry.to_dict()) for i, entry in save_agg_users().iterrows()]
    session.add_all(data)
    data = [Agg_Trans(**entry.to_dict()) for i, entry in save_agg_trans().iterrows()]
    session.add_all(data)
    data = [Agg_Ins(**entry.to_dict()) for i, entry in save_agg_ins().iterrows()]
    session.add_all(data)
    data = [Map_User(**entry.to_dict()) for i, entry in save_map_users().iterrows()]
    session.add_all(data)
    data = [Map_Trans(**entry.to_dict()) for i, entry in save_map_trans().iterrows()]
    session.add_all(data)
    data = [Map_Ins(**entry.to_dict()) for i, entry in save_map_ins().iterrows()]
    session.add_all(data)
    data = [Top_User(**entry.to_dict()) for i, entry in save_top_users().iterrows()]
    session.add_all(data)
    data = [Top_Trans(**entry.to_dict()) for i, entry in save_top_trans().iterrows()]
    session.add_all(data)
    data = [Top_Ins(**entry.to_dict()) for i, entry in save_top_ins().iterrows()]
    session.add_all(data)

    session.commit()
        
