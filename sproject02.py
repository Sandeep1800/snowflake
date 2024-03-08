import streamlit as st 
import pandas as pd
import botocore.exceptions
import boto3
import json
import time
from streamlit_extras.switch_page_button import switch_page
 

st.set_page_config(
    page_title="CLOUDTRAIL ANALYTICS",
    page_icon="📲", 
    layout="wide",
    initial_sidebar_state="expanded",)

 
st.image('aws.png',width=166)
st.markdown("<h1 style='color:#ff9900;'>CLOUDTRAIL ANALYTICS</h1>", unsafe_allow_html=True)
st.markdown(' ')
st.markdown(' ')
st.markdown(' ')
st.markdown(' ')
st.markdown("<h3 style='color:#242f3d;'>AWS CONNECTION</h3>", unsafe_allow_html=True)
video_file = open('CLOUDTRAIL ANALYTICS.mp4','rb')
video_bytes = video_file.read()

st.video(video_bytes)

access_key=st.text_input('Access_key')
secret_key=st.text_input('Secret_key')
region_name=st.text_input('Region_name')

if access_key and secret_key and region_name:
    s3 = boto3.resource(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key  
        )  
    st.success("Successfully connected to AWS.")
   
    bucket_names = []
    for bucket_name in s3.buckets.all():

        bucket_names.append(bucket_name.name)

    buckets = st.selectbox('Bucket Name', bucket_names)

    if buckets:
                try:
                    f_list = [obj.key for obj in s3.Bucket(buckets).objects.all()]
                
                    files = st.selectbox('Select file', f_list)
                    if files:
                        obj = s3.Bucket(buckets).Object(files).get()
                        df = pd.read_csv(obj['Body'])
                        df.to_csv('Data.csv', index=False)
                        st.dataframe(df)
                        
                    next_button = st.button('Next')
                    if next_button:
                        switch_page('Connect to Snowflake')
                except botocore.exceptions.NoCredentialsError:
                    st.error("Please check your AWS credentials")

    else:
                st.error("Could not connect to AWS")




    
