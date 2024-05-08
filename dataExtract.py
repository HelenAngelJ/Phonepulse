import json
import os
import mysql.connector

def connect_database():
    mydb = mysql.connector.connect(
        host="localhost",
        user="helen1",
        password="123456",
        database="phonepulse")
    return mydb

#creating tables if tables doesn't exists
#<------------------------------------------>
def create_tables():
    db = connect_database()
    cursor = db.cursor()
    cursor.execute("use phonepulse")
    
    try:
        cursor.execute("""
                    create table if not exists aggregated_transaction (
                    State varchar(255), 
                    yr year,
                    quater int,
                    transaction_category varchar(255),
                    count bigint,
                    amount double
                    )
                    """)
        result1 = cursor.warning_count
        if result1 == 0:
            print("aggregated_transaction table created") #st.success
            db.commit()

        cursor.execute("""
                    create table if not exists aggregated_user (
                    registeredUsers bigint,
                    appOpens bigint,
                    State varchar(255),
                    yr year,
                    quater int
                    )
                    """)
        result2 = cursor.warning_count
        if result2 == 0:
            print("aggregated_user table created") #st.success
            db.commit()
        
        cursor.execute("""
                create table if not exists map_transaction (
                count bigint,
                amount double,
                State varchar(255),
                yr year,
                quater int
                )
                """)
        result3 = cursor.warning_count
        if result3 == 0:
            print("map_transaction table created") #st.success
            db.commit()
            

        cursor.execute("""
                create table if not exists map_user (
                registeredUsers bigint,
                appOpens bigint,
                State varchar(255),
                yr year,
                quater int
                )
                """)
        result4 = cursor.warning_count
        if result4 == 0:
            print("map_user table created") #st.success
            db.commit()
            
        cursor.execute("""
                create table if not exists top_transaction_districts (
                districts varchar(255),
                count bigint,
                amount double,
                State varchar(255),
                yr year,
                quater int
                )
                """)
        result5 = cursor.warning_count
        if result5 == 0:
            print("top_transaction_districts table created") #st.success
            db.commit()
        
        cursor.execute("""
                create table if not exists top_transaction_pincodes (
                pincode int,
                count bigint,
                amount double,
                State varchar(255),
                yr year,
                quater int
                )
                """)
        result6 = cursor.warning_count
        if result6 == 0:
            print("top_transaction_pincodes table created") #st.success
            db.commit()
        

        cursor.execute("""
                create table if not exists top_user_districts (
                districts varchar(255),
                registeredUsers bigint,
                State varchar(255),
                yr year,
                quater int
                )
                """)
        result7 = cursor.warning_count
        if result7 == 0:
            print("top_user_districts table created") #st.success
            db.commit()
        
        cursor.execute("""
                create table if not exists top_user_pincodes (
                pincodes int,
                registeredUsers bigint,
                State varchar(255),
                yr year,
                quater int
                )
                """)
        result8 = cursor.warning_count
        if result8 == 0:
            print("top_user_pincodes table created") #st.success
            db.commit()
        
        cursor.execute('''
        SELECT count(*) AS TOTALNUMBEROFTABLES
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = "phonepulse"
        ''')
        result = cursor.fetchone()
        #print(result[0])

        cursor.close()

        return result[0]

    except mysql.connector.Error as err:
        print(f"mysql connetor error: {err}") #st.error


#checking for the tables in database
check = create_tables()

#inserting data
#<---------------->
def insert_data(table_name,data_value):
    match table_name:
        case "aggregated_transaction":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into aggregated_transaction (State,yr,quater,transaction_category,count,amount) values (%s,%s,%s,%s,%s,%s)
                            '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in aggregated_transaction : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting aggregated_transaction table : {e}')
        case "aggregated_user":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into aggregated_user (registeredUsers,appOpens,State,yr,quater) values (%s,%s,%s,%s,%s)
                            '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in aggregated_user : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting aggregated_user table : {e}')
        case "map_transaction":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into map_transaction (count,amount,State,yr,quater) values (%s,%s,%s,%s,%s)
                        '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in map_transaction : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting map_transaction table : {e}')
        case "map_user":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into map_user (registeredUsers,appOpens,State,yr,quater) values (%s,%s,%s,%s,%s)
                        '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in map_user table : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting map_user table : {e}')

        case "top_transaction_districts":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into top_transaction_districts (districts,count,amount,State,yr,quater) values (%s,%s,%s,%s,%s,%s)
                        '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in top_transaction_districts : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting top_transaction_districts : {e}')
        
        case "top_transaction_pincodes":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into top_transaction_pincodes (pincode,count,amount,State,yr,quater) values (%s,%s,%s,%s,%s,%s)
                        '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in top_transaction_pincodes : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting top_transaction_pincodes table : {e}')
        case "top_user_districts":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into top_user_districts (districts,registeredUsers,State,yr,quater) values (%s,%s,%s,%s,%s)
                        '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in top_user_districts : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting top_user_districts table : {e}')
        
        case "top_user_pincodes":
            
            try:
                db = connect_database()
                cursor = db.cursor()
                cursor.execute("use phonepulse")
                for value in data_value:
                    sql = '''
                            insert into top_user_pincodes (pincodes,registeredUsers,State,yr,quater) values (%s,%s,%s,%s,%s)
                        '''
                    cursor.execute(sql,value)
                db.commit()
                cursor.close()
                #st.success(f"data inserted in top_user_pincodes : {data_value}")
            except mysql.connector.Error as e:
                print(f'error inserting top_user_pincodes table : {e}')

# extracting the data and inserting to database
#<----------------------------------------------->
if check == 8:
    print("Tables are ready")

    section1 = ["aggregated","map","top"]
    section2 = ["transaction","user"]

    for category in section1:
        for sub_category in section2:
            if category == "map":
                path=f"pulse/data/{category}/{sub_category}/hover/country/india/state/"
            else:
                path=f"pulse/data/{category}/{sub_category}/country/india/state/"
            
            state_list=os.listdir(path)
            
            
            for i in state_list:
                p_i=path+i+"/"
                Agg_yr=os.listdir(p_i)
                for j in Agg_yr:
                    p_j=p_i+j+"/"
                    Agg_yr_list=os.listdir(p_j)
                    for k in Agg_yr_list:
                        p_k=p_j+k
                        Data=open(p_k,'r')
                        D=json.load(Data)
                        match category:
                            case "aggregated":
                                match sub_category:
                                    case "transaction":
                                        data_list = []
                                        for z in D['data']['transactionData']:
                                            data = {
                                                'State' : i,
                                                'year' : j,
                                                'quater' : int(k.strip('.json')),
                                                'transaction_category' : z['name'],
                                                'count' : z['paymentInstruments'][0]['count'],
                                                'amount' : z['paymentInstruments'][0]['amount']
                                            }
                                            value = (data['State'],data['year'],data['quater'],data['transaction_category'],data['count'],data['amount'])
                                            data_list.append(value)

                                        insert_data("aggregated_transaction",data_list)

                                    case "user":
                                        data_list1 = []
                                        for z in D['data']['aggregated']:
                                            d1 = {
                                            'registeredUsers':  D['data']['aggregated']['registeredUsers'],
                                            'appOpens' : D['data']['aggregated']['appOpens'],
                                            'State' : i,
                                            'year' : j,
                                            'quater' : int(k.strip('.json'))
                                            }
                    
                                            value1 = (d1['registeredUsers'],d1['appOpens'],d1['State'],d1['year'],d1['quater'])
                                            data_list1.append(value1)

                                        insert_data("aggregated_user",data_list1)

                            case "map":
                                match sub_category:
                                    case "transaction":
                                        data_list = []
                                        for z in D['data']['hoverDataList']:
                                            d2 = {
                                            'count':  z['metric'][0]['count'],
                                            'amount': z['metric'][0]['amount'],
                                            'State' : i,
                                            'year' : j,
                                            'quater' : int(k.strip('.json'))
                                            }
                                            
                                            value2 = (d2['count'],d2['amount'],d2['State'],d2['year'],d2['quater'])
                                            data_list.append(value2)

                                        insert_data("map_transaction",data_list)

                                    case "user":
                                        data_list = []
                                        for z in D['data']['hoverData']:
                                            d3 = {
                                                'registeredUsers':  D['data']['hoverData'][z]['registeredUsers'],
                                                'appOpens': D['data']['hoverData'][z]['appOpens'],
                                                'State' : i,
                                                'year' : j,
                                                'quater' : int(k.strip('.json'))
                                            }
                                            
                                            value3 = (d3['registeredUsers'],d3['appOpens'],d3['State'],d3['year'],d3['quater'])
                                            data_list.append(value3)

                                        insert_data("map_user",data_list)

                            case "top":
                                match sub_category:
                                    case "transaction":
                                        data_list = []
                                        for z in D['data']['districts']:
                                            data4 = {
                                                'districts' : z['entityName'],
                                                'count' : z['metric']['count'],
                                                'amount' : z['metric']['amount'],
                                                'State' : i,
                                                'year' : j,
                                                'quater' : int(k.strip('.json'))
                                            }
                                            
                                            value4 = (data4['districts'],data4['count'],data4['amount'],data4['State'],data4['year'],data4['quater'])
                                            data_list.append(value4)

                                        insert_data("top_transaction_districts",data_list)

                                        data_list1 = []
                                        for z in D['data']['pincodes']:
                                            data5 = {
                                                'pincode' : z['entityName'],
                                                'count' : z['metric']['count'],
                                                'amount' : z['metric']['amount'],
                                                'State' : i,
                                                'year' : j,
                                                'quater' : int(k.strip('.json'))
                                            }
                                            
                                            value5 = (data5['pincode'],data5['count'],data5['amount'],data5['State'],data5['year'],data5['quater'])
                                            data_list1.append(value5)

                                        insert_data("top_transaction_pincodes",data_list1)
                                            
                                    case "user":
                                        data_list = []
                                        for z in D['data']['districts']:
                                            data6 = {
                                                'districts' : z['name'],
                                                'registeredUsers' :  z['registeredUsers'],
                                                'State' : i,
                                                'year' : j,
                                                'quater' : int(k.strip('.json'))
                                            }
                                            
                                            value6 = (data6['districts'],data6['registeredUsers'],data6['State'],data6['year'],data6['quater'])
                                            data_list.append(value6)

                                        insert_data("top_user_districts",data_list)

                                        data_list1 = []
                                        for z in D['data']['pincodes']:
                                            data7 = {
                                                'pincodes' : z['name'],
                                                'registeredUsers' :  z['registeredUsers'],
                                                'State' : i,
                                                'year' : j,
                                                'quater' : int(k.strip('.json'))
                                            }
                                            
                                            value7 = (data7['pincodes'],data7['registeredUsers'],data7['State'],data7['year'],data7['quater'])
                                            data_list1.append(value7)

                                        insert_data("top_user_pincodes",data_list1)

