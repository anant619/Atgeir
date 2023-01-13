from importlib.resources import contents
import os
import sys
import configparser
import yaml
import boto3
import subprocess
import shlex
from neo4j import GraphDatabase
import snowflake.connector

sys.path.append("../")

def snow_connect(sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, sf_schema):
    
    connection = snowflake.connector.connect(account=sf_account, user=sf_user, password=sf_password,role=sf_role, 
                                             warehouse=sf_warehouse, database=sf_database, schema=sf_schema,
                                             timezone='UTC')
    print("connected to Snowflake")
    return connection

def read_config_file(config_dir):
    """ 
    Sourcing Configuration File
    """
    try:
        conf = configparser.RawConfigParser()
        conf.read(config_dir)
    except Exception as ex:
        # logging.error(f"Error code    = {type(ex).__name__}")
        # logging.error(f"Error Message = {ex}")
        # sys.exit(1)
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")
    return conf


def get_graph_driver(host_port, username, password):
    try:
        driver = GraphDatabase.driver(host_port,auth=(username,password))
        return driver
    except Exception as ex:
        # logging.error(f"Error code    = {type(ex).__name__}")
        # logging.error(f"Error Message = {ex}")
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")
