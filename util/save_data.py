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


agg_trans_states = 'pulse/data/aggregated/transaction/country/india/state'
agg_users_states = 'pulse/data/aggregated/user/country/india/state'
map_trans_states = 'pulse/data/map/transaction/hover/country/india/state'
map_users_states = 'pulse/data/map/user/country/hover/india/state'
top_trans_states = 'pulse/data/top/transaction/country/india/state'
top_users_states = 'pulse/data/top/user/country/india/state'


def save_agg_trans():
    agg_trans = []
    for state in os.listdir(agg_trans_states):
        years = agg_trans_states+state+'/'
        for year in os.listdir(years):
            quarters = years+year+'/'
            for file in os.listdir(quarters):
                file_path = quarter+file
                with open(file_path) as json_file:
                    data = json.load(json_file)
                for payments in data['data']['transactionData']:
                    quarter = int(file.strip('.json'))
                    agg_trans.append({
                        'State':state,
                        'Year':year,
                        'Quarter':quarter,
                        'Transaction_Name':payments['name'],
                        'Count':payments['paymentInstruments'][0]['count'],
                        'Amount':payments['paymentInstruments'][0]['amount'],
                    })
    return pd.DataFrame(agg_trans)


def save_agg_users():
    agg_users = []
    for state in os.listdir(agg_users_states):
        years = agg_users_states+state+'/'
        for year in os.listdir(years):
            quarters = years+year+'/'
            for file in os.listdir(quarters):
                file_path = quarter+file
                with open(file_path) as json_file:
                    data = json.load(json_file)
                for brand_data in data['data']['usersByDevice']:
                    quarter = int(file.strip('.json'))
                    agg_users.append({
                        'State':state,
                        'Year':year,
                        'Quarter':quarter,
                        'Registered_Users':data['data']['aggregated']['registeredUsers'],
                        'App_Opens':data['data']['aggregated']['appOpens'],
                        'Brand':brand_data['brand'],
                        'Count':brand_data['count'],
                        'Percentage':brand_data['percentage'],
                    })
    return pd.DataFrame(agg_users)


def save_map_trans():
    map_trans = []
    for state in os.listdir(map_trans_states):
        years = map_trans_states+state+'/'
        for year in os.listdir(years):
            quarters = years+year+'/'
            for file in os.listdir(quarters):
                file_path = quarter+file
                with open(file_path) as json_file:
                    data = json.load(json_file)
                for entry in data['data']['hoverDataList']:
                    quarter = int(file.strip('.json'))
                    map_trans.append({
                        'State':state,
                        'Year':year,
                        'Quarter':quarter,
                        'District':entry['name'],
                        'Count':entry['metric'][0]['count'],
                        'Amount':entry['metric'][0]['amount'],
                    })
    return pd.DataFrame(map_trans)



def save_map_users():
    map_users = []
    for state in os.listdir(map_users_states):
        years = map_users_states+state+'/'
        for year in os.listdir(years):
            quarters = years+year+'/'
            for file in os.listdir(quarters):
                file_path = quarter+file
                with open(file_path) as json_file:
                    data = json.load(json_file)
                for district, values in data["data"]["hoverData"].items():
                    quarter = int(file.strip('.json'))
                    map_users.append({
                        "State":state,
                        "Year":year,
                        "Quarter":quarter,
                        "District": district if district else 'NA',
                        "Registered_Users": values["registeredUsers"],
                        "App_Opens": values["appOpens"],
                    })
    return pd.DataFrame(map_users)


def save_top_trans():
    top_trans_districts = []
    top_trans_pincodes = []
    for state in os.listdir(top_trans_states):
        years = top_trans_states+state+'/'
        for year in os.listdir(years):
            quarters = years+year+'/'
            for file in os.listdir(quarters):
                file_path = quarter+file
                with open(file_path) as json_file:
                    data = json.load(json_file)
                for district in data['data']['districts']:
                    quarter = int(file.strip('.json'))
                    top_trans_districts.append({
                        'State':state,
                        'Year':year,
                        'Quarter':quarter,
                        'District':district['entityName'],
                        'Count':district['metric']['count'],
                        'Amount':district['metric']['amount'],
                    })
                
                for pincode in data['data']['pincodes']:
                    quarter = int(file.strip('.json'))
                    top_trans_pincodes.append({
                        'State':state,
                        'Year':year,
                        'Quarter':quarter,
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
    for state in os.listdir(top_users_states):
        years = top_users_states+state+'/'
        for year in os.listdir(years):
            quarters = years+year+'/'
            for file in os.listdir(quarters):
                file_path = quarter+file
                with open(file_path) as json_file:
                    data = json.load(json_file)
                for district in data['data']['districts']:
                    quarter = int(file.strip('.json'))
                    top_user_districts.append({ 
                        'State':state,
                        'Year':year,
                        'Quarter':quarter,
                        'District':district['name'],
                        'Registered_Users':district['registeredUsers'],
                    })
                for pincode in data['data']['pincodes']:
                    quarter = int(file.strip('.json'))
                    top_user_pincodes.append({ 
                        'State':state,
                        'Year':year,
                        'Quarter':quarter,
                        'PIN':pincode['name'],
                        'Registered_Users':pincode['registeredUsers'],
                    })
    top_user_district_df = pd.DataFrame(top_user_districts)
    top_user_pincodes_df = pd.DataFrame(top_user_pincodes)
    return top_user_district_df,top_user_pincodes_df



def save_data():
    cnx = mysql.connector.connect(
        user=aws_db_usr,
        password=aws_db_pwd,
        host=aws_db_endpoint,
        database=aws_db_dbname
    )
    
    save_agg_trans(cnx)
    save_agg_users(cnx)
    save_map_trans(cnx)
    save_map_users(cnx)
    save_top_trans(cnx)
    save_top_users(cnx)

    cnx.close()

    