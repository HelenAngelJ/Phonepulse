import pandas as pd
import json
import os
import mysql.connector
import streamlit as st
from streamlit_navigation_bar import st_navbar
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout='wide')


#defining the navigation bar
page = st_navbar(["Explore", "Reports"],logo_path="static/logo.svg")

#initializing session state for scrap_button
def button(button_name):
    if button_name not in st.session_state:
        st.session_state[button_name] = False

def click_button(button_name):
    st.session_state[button_name] = True

button("explore")

if page == "Home":
    with st.container(border=True):
        st.header(":violet[Phonepe Pulse]")
        st.subheader("walk through the trends")
        st.video("https://www.youtube.com/watch?v=Yy03rjSUIB8&t=14s",'rb')

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


button("apply")

#from database tables to dataframe
#------------------------------------#
@st.cache_data
def aggre_transaction(yr,qtr):
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from aggregated_transaction")
    result1 = cursor.fetchall()
    cursor.close()
    agg_tran_df = pd.DataFrame(result1,columns=["state","year","quater","transaction_type","transaction_count","transaction_amount"])

    agg_year = agg_tran_df[agg_tran_df['year']==yr]
    agg_quater = agg_year[agg_year['quater'] == qtr]
    agg_quater.reset_index(drop=True,inplace=True)
    state = change_state(agg_quater)
    agg_quater['state'] = agg_quater['state'].replace(state[0],state[1])
    values = dict(total_transaction = agg_quater['transaction_count'].sum(),
                    total_tran_amount = agg_quater['transaction_amount'].sum(),
                    mean_tran_amount = agg_quater['transaction_amount'].mean()
                )
    agg_tran = agg_tran_df.groupby("transaction_type",as_index=False)[['transaction_count']].sum()
    agg_tran = agg_tran.to_dict('series')

    #calculating top states
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute(f"select State,yr,quater,amount from aggregated_transaction where yr = {yr} and quater = {qtr}")
    result9 = cursor.fetchall()
    cursor.close()
    
    agg_top_state = pd.DataFrame(result9,columns=['state','year','quater','amount'])
    agg_top_state = agg_top_state.groupby(['state'],as_index=False)[['amount']].sum()
    state_name = change_state(agg_top_state)
    agg_top_state['state'] = agg_top_state['state'].replace(state_name[0],state_name[1])
    agg_top_state.sort_values('amount',ascending=False,inplace=True)
    agg_top = agg_top_state['amount'].apply(converter)
    agg_top_state.replace(list(agg_top_state['amount']),list(agg_top),inplace=True)

    return agg_quater,values,agg_tran,agg_top_state

@st.cache_data
def aggre_user(year,quater):

    #total registered users till last quater
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select sum(registeredUsers) from aggregated_User")
    result2 = cursor.fetchone()
    cursor.close()

    reg_user = result2[0]

    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute(f"select * from aggregated_user where yr = {year} and quater = {quater}")
    result2 = cursor.fetchall()
    cursor.close()
    agg_user_df = pd.DataFrame(result2,columns=["registeredUsers","appOpens","state","year","quater"])

    #total reguser and total appopens
    agg_user_df = agg_user_df.groupby(['state','year','quater'],as_index=False)[['registeredUsers','appOpens']].aggregate(sum)

    #changing the state name to match the geojson
    state = change_state(agg_user_df)
    agg_user_df['state'] = agg_user_df['state'].replace(state[0],state[1])

    return agg_user_df,reg_user

@st.cache_data(show_spinner=False)
def map_transaction(yr,qtr):
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from map_transaction")
    result3 = cursor.fetchall()
    cursor.close()
    map_df =  pd.DataFrame(result3,columns=["count","amount","state","year","quater"])


    map_year = map_df[map_df["year"] == yr]
    map_quater = map_year[map_year['quater'] == qtr]
    map_quater.reset_index(drop=True,inplace=True)
    map_mean = map_quater.groupby(["state","year","quater"],as_index=False).mean()
    map_mean.rename(columns ={'count':'count_mean','amount':'amount_mean'},inplace=True)
    map_sum = map_quater.groupby(['state','year','quater'],as_index=False).sum()
    map_sum.rename(columns={'count':'count_sum','amount':'amount_sum'},inplace=True)
    df = map_mean.merge(map_sum,on=['state','year','quater'],how='left')
    state = change_state(df)
    df['state'] = df['state'].replace(state[0],state[1])
    
    return df

@st.cache_data
def map_user(year,quatr):
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute(f"select * from map_user where yr = {year} and quater = {quatr}")
    result4 = cursor.fetchall()
    cursor.close()
    map_user_df =  pd.DataFrame(result4,columns=["registeredUsers","appOpens","state","year","quater"])

    map_user_df = map_user_df.groupby(['state','year','quater'],as_index=False)[['registeredUsers','appOpens']].sum()
    #change the state name
    state = change_state(map_user_df)
    map_user_df['state'] = map_user_df['state'].replace(state[0],state[1])

    #top state
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute(f'''select State,sum(appOpens) as total
                    from aggregated_user 
                    where yr={year} and quater={quatr}
                    group by State
                    order by total desc''')
    result41 = cursor.fetchall()
    cursor.close()
    top_state_user_df =  pd.DataFrame(result41,columns=["state","appOpens"])

    #change state name
    state = change_state(top_state_user_df)
    top_state_user_df['state'] = top_state_user_df['state'].replace(state[0],state[1])

    return map_user_df,top_state_user_df

@st.cache_data
def top_tran_distr(yr,qtr):
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select * from top_transaction_districts")
    result5 = cursor.fetchall()
    cursor.close()
    top_trans_dist =  pd.DataFrame(result5,columns=["districts","count","amount","state","year","quater"])

    top_year = top_trans_dist[top_trans_dist['year']==yr]
    top_quater = top_year[top_year['quater'] == qtr]
    top_quater.reset_index(drop=True,inplace=True)
    top_tran = top_quater.groupby(["districts","state"],as_index=False)[['amount']].sum()
    top_tran.sort_values('amount',ascending=False,inplace=True,ignore_index=True)

    return top_tran

@st.cache_data
def top_tran_pin(yr,qtr):
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute(f"select * from top_transaction_pincodes where yr = {yr} and quater = {qtr}")
    result6 = cursor.fetchall()
    cursor.close()
    top_trans_pincode = pd.DataFrame(result6,columns=["pincode","count","amount","state","year","quater"])

    top_trans_pincode = top_trans_pincode.groupby(["pincode","state"],as_index=False)[['amount']].sum()
    top_trans_pincode.sort_values('amount',ascending=False,inplace=True,ignore_index=True)
    state = change_state(top_trans_pincode)
    top_trans_pincode['state'] = top_trans_pincode['state'].replace(state[0],state[1])

    return top_trans_pincode

@st.cache_data
def top_user_distr(yer,quat):
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute(f'''select districts,State,sum(registeredUsers) as total
                        from top_user_districts
                        where yr = {yer} and quater = {quat}
                        group by districts,State
                        order by total desc''')
    result7 = cursor.fetchall()
    cursor.close()
    top_user_dist = pd.DataFrame(result7,columns=["districts",'state',"registeredUsers"])

    return top_user_dist

@st.cache_data
def top_user_pin(yer,qutr):
    db = connect_database()
    cursor = db.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute(f'''
                    select pincodes,State,sum(registeredUsers) as total
                    from top_user_pincodes
                    where yr = {yer} and quater = {qutr}
                    group by pincodes,State
                    order by total desc
                ''')
    result8 = cursor.fetchall()
    cursor.close()
    top_user_pincode = pd.DataFrame(result8,columns=["pincode",'state',"registeredUsers"])
    return top_user_pincode

    
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

def converter(a):
    a = float(a)/1000000000
    return format(a,".2f")

def amount_formater(val):
    amt_atr = str(val)
    value = amt_atr.split('.')
    amt = value[0]
    if len(amt) > 3:
        amt = amt[0:len(amt)-3]+","+amt[len(amt)-3:len(amt)]+"."+value[1]
    return amt

with open("C:/Users/USER/py/phonepluse/india_states.geojson") as response:
    geo = json.load(response)

if page == "Explore":
    col1, col2 = st.columns([0.3,0.7])
    with col1:
        with st.container(border=True):
            type = st.selectbox("type",["Transaction","User"],label_visibility="hidden")
            year = st.selectbox("year",[2023,2022,2021,2019,2018],label_visibility="hidden")
            Q = ["Q1(Jan-Mar)","Q2(Apr-Jun)","Q3(Jul-Sep)","Q4(Oct-Dec)"]
            quater = st.selectbox("quater",Q,label_visibility="hidden")
            apply = st.button("apply",on_click=click_button("apply"))

    with col2:
        with st.container():
            if apply:
                if type == "Transaction":
                    df = map_transaction(year,Q.index(quater)+1)
                    fig = px.choropleth_mapbox(df,
                                locations='state',
                            geojson=geo,
                            featureidkey='properties.ST_NM',
                            color='amount_sum',
                            hover_name = 'state',
                            hover_data=['count_sum','amount_mean','amount_sum'],
                            mapbox_style='carto-positron',
                            center={'lat':24,'lon':78},
                            zoom=3.7,
                            color_continuous_scale=px.colors.diverging.Portland_r,
                            color_continuous_midpoint=df['count_sum'].median(),
                            width=800,
                            height=600
                                )
                    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
                    st.plotly_chart(fig)
                if type == "User":
                    map_user_df,top_state_user = map_user(year,Q.index(quater)+1)
                    fig1 = px.choropleth_mapbox(map_user_df,
                                locations='state',
                            geojson=geo,
                            featureidkey='properties.ST_NM',
                            color='appOpens',
                            hover_name = 'state',
                            hover_data=["registeredUsers","appOpens"],
                            mapbox_style='carto-positron',
                            center={'lat':24,'lon':78},
                            zoom=3.7,
                            color_continuous_scale=px.colors.diverging.Picnic_r,
                            color_continuous_midpoint=map_user_df['appOpens'].median(),
                            width=800,
                            height=600
                                )
                    fig1.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
                    st.plotly_chart(fig1)

    with st.container(border=True):
            if apply:
                if type == "Transaction":
                    df,value,categories,top_state = aggre_transaction(year,Q.index(quater)+1)
                    st.header(":blue[Transactions]")
                    st.subheader("All PhonePe transactions (UPI + Cards + Wallets)")
                    st.subheader(":blue["+f"{value['total_transaction']}"+"]")
                    col1,col2 = st.columns([0.5,0.5])
                    with col1:
                        st.subheader("Total payment value")
                        st.subheader(":blue["+f"{value['total_tran_amount']}"+"]")
                        st.subheader("Avg. transaction value")
                        st.subheader(":blue["+f"{value['mean_tran_amount']}"+"]")
                    with col2:
                        st.subheader("Categories")
                        col1, col2 = st.columns(2,gap="small")
                        with col1:
                            for i in categories['transaction_type']:
                                st.text(i)
                        
                        with col2:
                            for i in categories['transaction_count']:
                                st.write(":blue["+f"{i}"+"]")
                    
                    st.divider()
                    tab1, tab2, tab3 = st.tabs(["State", "District", "Postal Code"])
                    with tab1:
                        st.subheader("Top 10 State")
                        col1,col2,col3= st.columns([0.15,0.15,0.7])

                        with col1:
                            for i in range(0,10):
                                name = list(top_state['state'])
                                st.write(f"{i+1})"+f"{name[i]}")
                        with col2:
                            for i in range(0,10):
                                amt = list(top_state['amount'])
                                amnt = amount_formater(amt[i])                       
                                st.write(":blue["+f"{amnt} B"+"]")
                        with col3:
                            fig = px.pie(top_state, values='amount', names='state')
                            st.plotly_chart(fig)
                    with tab2:
                        st.subheader("Top 10 District")
                        top_district = top_tran_distr(year,Q.index(quater)+1)
                        col1,col2,col3 = st.columns([0.2,0.2,0.6])
                        with col1:
                            for i in range(0,10):
                                name = [i.capitalize() for i in list(top_district['districts'])]
                                st.write(f"{i+1})"+f"{name[i]}")
                        with col2:
                            for i in range(0,10):
                                amt = list(top_district['amount'])
                                amnt = amount_formater(amt[i])                       
                                st.write(":blue["+f"{amnt} B"+"]")
                        with col3:
                            fig = px.bar(top_district,
                                        x = "districts",
                                        y = 'amount',
                                        hover_data= ["districts",'amount'],
                                        color='state',
                                        facet_row_spacing=1
                                        )
                            st.plotly_chart(fig)
                    with tab3:
                        st.subheader("Top 10 Postal Codes")
                        top_postalcode = top_tran_pin(year,Q.index(quater)+1)
                        col1,col2,col3 = st.columns([0.15,0.2,0.65])

                        with col1:
                            for i in range(0,10):                       
                                st.write(f"{i+1})"+f"{top_postalcode['pincode'][i]}")

                        with col2:
                            for i in range(0,10):
                                amt = list(top_postalcode['amount'])
                                amnt = amount_formater(amt[i])                       
                                st.write(":blue["+f"{amnt} B"+"]")

                        with col3:

                            fig = px.bar(top_postalcode,
                                        x = 'amount',
                                        y = "state",
                                        hover_data= ["pincode",'state','amount'],
                                        color='state',
                                        facet_row_spacing=1,
                                        facet_col_spacing=1
                                        )
                            
                            st.plotly_chart(fig)
                if type == "User":
                    user_df,total_reg_user = aggre_user(year,Q.index(quater)+1)
                    st.header("Users")
                    st.subheader(":violet[Registered PhonePe Users till "+f"{quater}{year}]")
                    st.subheader(total_reg_user)
                    st.subheader(":violet[appOpens in "+f"{quater}{year}]")
                    app_opens = user_df['appOpens'].sum()
                    st.subheader(app_opens)

                    st.divider()
                    tab1, tab2, tab3 = st.tabs(["State", "District", "Postal Code"])
                    with tab1:
                        st.subheader(":violet[Top 10 State]")
                        col1,col2,col3= st.columns([0.15,0.15,0.7])

                        with col1:
                            for i in range(0,10):
                                name = list(top_state_user['state'])
                                st.write(f"{i+1})"+f"{name[i]}")
                        with col2:
                            for i in range(0,10):
                                appOpen = top_state_user['appOpens'][i]                  
                                st.write(":violet["+f"{appOpen}"+"]")
                        with col3:
                            fig = px.pie(top_state_user, values='appOpens', names='state')
                            st.plotly_chart(fig)
                    with tab2:
                        st.subheader(":violet[Top 10 District]")
                        top_user_district = top_user_distr(year,Q.index(quater)+1)
                        col1,col2,col3 = st.columns([0.2,0.2,0.6])
                        with col1:
                            for i in range(0,10):
                                name = [i.capitalize() for i in list(top_user_district['districts'])]
                                st.write(f"{i+1})"+f"{name[i]}")
                        with col2:
                            for i in range(0,10):
                                reg_user = top_user_district['registeredUsers'][i]                    
                                st.write(":violet["+f"{reg_user}"+"]")
                        with col3:
                            fig = px.bar(top_user_district,
                                        x = "districts",
                                        y = 'registeredUsers',
                                        hover_data= ["districts",'state','registeredUsers'],
                                        color='state',
                                        facet_row_spacing=1
                                        )
                            st.plotly_chart(fig)
                    with tab3:
                        st.subheader("Top 10 Postal Codes")
                        topuser_postalcode = top_user_pin(year,Q.index(quater)+1)
                        col1,col2,col3 = st.columns([0.15,0.2,0.65]) #"pincode","registeredUsers"

                        with col1:
                            for i in range(0,10):                       
                                st.write(f"{i+1})"+f"{topuser_postalcode['pincode'][i]}")

                        with col2:
                            for i in range(0,10):
                                users = topuser_postalcode['registeredUsers'][i]                   
                                st.write(":violet["+f"{users}"+"]")

                        with col3:

                            fig = px.bar(topuser_postalcode,
                                        x = 'state',
                                        y = "registeredUsers",
                                        hover_data= ["pincode",'registeredUsers'],
                                        color='state',
                                        facet_row_spacing=1,
                                        facet_col_spacing=1
                                        )
                            
                            st.plotly_chart(fig)

                    
if page == "Reports":
    with st.container():
        with st.container(border=True):
            col1,col3 = st.columns([0.3,0.7])
            with col1:
                st.header("Q4 2021 (Oct - Dec) Report")
                st.write('''
                The PhonePe Pulse report showcased that merchant transactions registered a robust QoQ growth driven by the holiday season, 
                        festivities and multiple e-commerce shopping sales.
                ''')
                col1,col2 = st.columns(2)
                with col1:
                    with open("static\Pulse_Report_Q4_2021_M_B.pdf", "rb") as file:
                        btn1 = st.download_button(
                                label="dowload Report",
                                data=file,
                                file_name="Pulse_Report_Q4_2021_M_B"
                            )
                with col2:
                    with open("static\Pulse_Report_Q4_2021_M_B (1).pdf", "rb") as file:
                        btn1 = st.download_button(
                                label="download report",
                                data=file,
                                file_name="Pulse_Report_Q4_2021_M_B (1)"
                            )
            with col3:
                st.image("static\phoneImage.png")         
        
