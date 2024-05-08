import pandas as pd
import json
import os
import mysql.connector
import streamlit as st
from streamlit_navigation_bar import st_navbar
import plotly.express as px

#defining the navigation bar
page = st_navbar(["Home", "Documentation", "Examples", "Community", "About"],logo_path="static/logo.svg",logo_page="Phonepe pluse")

# database connection
#<-------------------------->
@st.cache_resource
def connect_database():
    mydb = mysql.connector.connect(
        host=st.secrets["host"],
        user=st.secrets["user"],
        password=st.secrets["password"],
        database=st.secrets["database"])
    return mydb

#initializing session state for scrap_button
def button(button_name):
    if button_name not in st.session_state:
        st.session_state[button_name] = False

def click_button(button_name):
    st.session_state[button_name] = True

button("apply")

#from database tables to dataframe
#------------------------------------#
@st.cache_data
def aggre_transaction():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from aggregated_transaction")
    result1 = cursor.fetchall()
    cursor.close()
    agg_tran_df = pd.DataFrame(result1,columns=["state","year","quater","transaction_type","transaction_count","transaction_amount"])
    return agg_tran_df

@st.cache_data
def aggre_user():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from aggregated_user")
    result2 = cursor.fetchall()
    cursor.close()
    agg_user_df = pd.DataFrame(result2,columns=["registeredUsers","appOpens","state","year","quater"])
    return agg_user_df

@st.cache_data
def map_transaction():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from map_transaction")
    result3 = cursor.fetchall()
    cursor.close()
    map_tran_df =  pd.DataFrame(result3,columns=["count","amount","state","year","quater"])

    return map_tran_df

@st.cache_data
def map_user():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from map_user")
    result4 = cursor.fetchall()
    cursor.close()
    map_user_df =  pd.DataFrame(result4,columns=["registeredUsers","appOpens","state","year","quater"])
    return map_user_df

@st.cache_data
def top_tran_distr():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from top_transaction_districts")
    result5 = cursor.fetchall()
    cursor.close()
    top_trans_dist =  pd.DataFrame(result5,columns=["districts","count","amount","state","year","quater"])
    return top_trans_dist

@st.cache_data
def top_tran_pin():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from top_transaction_pincodes")
    result6 = cursor.fetchall()
    cursor.close()
    top_trans_pincode = pd.DataFrame(result6,columns=["pincode","count","amount","state","year","quater"])
    return top_trans_pincode

@st.cache_data
def top_user_distr():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from top_user_districts")
    result7 = cursor.fetchall()
    cursor.close()
    top_user_dist = pd.DataFrame(result7,columns=["districts","registeredUsers","state","year","quater"])
    return top_user_dist

@st.cache_data
def top_user_pin():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from top_user_pincodes")
    result8 = cursor.fetchall()
    cursor.close()
    top_user_pincode = pd.DataFrame(result8,columns=["pincode","registeredUsers","state","year","quater"])
    return top_user_pincode

def map(df):
    fig = px.choropleth(
        df,
        geojson="india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='state',
        color='count_sum',
        color_continuous_scale='Reds'
        )

    fig.update_geos(fitbounds="locations", visible=False)
    return fig
    
@st.cache_data
def change_state(df):
    state_old = []
    for x in df['state']:
        state_old.append(x)
    state = []
    state_exclude = ["andaman-&-nicobar-islands","jammu-&-kashmir"]
    for i in df['state']:
        
        if i not in state_exclude:
            tit = i.title()
            rem = tit.replace("&","and")
            new = rem.replace("-"," ")
        else:
            if i == state_exclude[0]:
                rem = i.replace("-islands","")
                tit = rem.title()
                new = tit.replace("-"," ")
            else:
                tit = i.title()
                new = tit.replace("-"," ")

        state.append(new)
    return_list = []
    return_list.append(state_old)
    return_list.append(state)
    return return_list

col1, col2 = st.columns([0.3,0.7])
with col1:
    with st.container(border=True):
        type = st.selectbox("type",["Transaction","User"],label_visibility="hidden")
        year = st.selectbox("year",[2023,2022,2021,2019,2018],label_visibility="hidden")
        Q = ["Q1(Jan-Mar)","Q2(Apr-Jun)","Q3(Jul-Sep)","Q4(Oct-Dec)"]
        quater = st.selectbox("quater",Q,label_visibility="hidden")
        apply = st.button("apply",on_click=click_button("apply"))

with st.container():
    if apply:
        if year == "Transaction":
            map_df = map_transaction()
            map_year = map_df[map_df["year"] == year]
            map_df_mean = map_year.groupby(["state","quater","year"]).mean()
            map_df_sum = map_year.groupby(["state","quater","year"]).sum()
            map_df_mean.rename(columns={'count':'count_mean','amount':'amount_mean'},inplace=True)
            map_df_sum.rename(columns={'count':'count_sum','amount':'amount_sum'},inplace=True)
            mapDf = map_df_mean.merge(map_df_sum,on=['state','year','quater'],how='left')
            state = change_state()
            mapDf['state'] = mapDf['state'].replace(state[0],state[1])
            st.dataframe(mapDf)
            result = map(mapDf)
            result.show()