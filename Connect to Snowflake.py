import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
import time
st.set_page_config(
    page_title="Snowflake Connection",
    page_icon="❄️", 
    layout="wide",
    initial_sidebar_state="expanded",)


st.markdown("<h4 style='color:#ff9900;'>Snowflake Connection</h4>", unsafe_allow_html=True)


username = st.text_input("Username")
password = st.text_input("Password", type="password")
account = st.text_input("Account Name")


if username and password and account:
      conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account
      )
      
      cur = conn.cursor()
      databases = cur.execute("SHOW DATABASES").fetchall()
      database_names = [db[1] for db in databases]
      database = st.selectbox('Database', database_names)
     
      schemas = cur.execute("SHOW SCHEMAS").fetchall()
      schema_names = [schema[1] for schema in schemas]
      schema = st.selectbox('Schema', schema_names)
      table_name= st.text_input('Enter Table Name')
      df= pd.read_csv('Data.csv')
      load= st.button('Load')
      if load:
         conn = snowflake.connector.connect(
                user=username,
                password=password,
                account=account,
                warehouse = 'COMPUTE_WH',
                database=database,
                schema=schema
            )
         
         success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper(), auto_create_table=True)
         with st.spinner('Uploading table to database '+database + ' in Snowflake'):
                time.sleep(5)
                conn.close()
                st.success("File uploaded to Snowflake successfully!")

