import os
import streamlit as st
#from dotenv import load_dotenv

#load_dotenv()

config = {
    "AWS_ACCESS_KEY_ID": st.secrets["aws_access_key"],
    'AWS_REGION': st.secrets["aws_region"],
    'AWS_SECRET_ACCESS_KEY': st.secrets["aws_secret_key"]
}