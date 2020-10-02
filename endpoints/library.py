import boto3
import pymysql
import json
import os
import calendar
import time
import datetime
import requests
from jose import jwt
from botocore.exceptions import ClientError

class Library():
  
  def __init__(self):
    config_file = os.path.join('configs', 'dev-config.json')
    with open(config_file) as f:
      self.config = json.load(f)
      self.ssl = {'ca' : self.config['ssl']}
    with open('directory.json') as f:
      self.directory = json.load(f) 
    with open('responses.json') as f:
      self.responses = json.load(f)
    
  def mysql_token_refresh(self):
    """
    *** FAKED ***
    Refreshes the AWS authentication token for the database
    """
    return 1

  def mysql_authenticate(self):
    """
    *** FAKED ***
    Authenticates and returns an open connection to the database
    """
    token = self.mysql_token_refresh()
    connection = {'token': token, 'connected': True}      
    return connection
  
  def converter(self, o):
    if isinstance(o, datetime.datetime):
      return o.__str__()
    
  def make_query(self, conn, statement, state_id):
    """
    *** FAKED ***
    Makes the query using the given connection and statement. In real code, there is no state_id in the signature
    This is used to fake a response
    """
    response = self.responses["responses"][state_id]
    return response
  
  def validate_token(self, req):
    """
    *** FAKED ***
    Validates the user's security token using another microservice
    """

    response = {'Result': {
      'email': 'abc@origen.ai',
      'custom:client': 'dev'
    }}
    return response, 200
  
  def submit_job(self, name, params):
    """
    *** FAKED ***
    Submits a job to the job queue. Returns a dictionary object with a job_id entry
    """
    return {'job_id': 'xyz123'}

  def clean_json(self, result):
    """
    Cleans the json by converting any non-serializable object into a serializable one, using the custom converter
    """      
    clean_result = json.dumps(result, default=self.converter)
    # loads the result back out, to get rid of the \" 
    return json.loads(clean_result)