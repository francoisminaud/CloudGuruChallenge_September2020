import csv
from datetime import datetime
import requests
import pandas as pd
import time
#for DynamoDB interaction
import boto3

nyt_file = "tmp/nyt_us.csv"
hopkins_file = "tmp/hopkins.csv"

#delete file if existent - to be added as module function (delete file)
#import os
#if os.path.isfile(nyt_file):
#    os.remove(nyt_file)
#    print(nyt_file+" File Removed!")
#if os.path.isfile(hopkins_file):
#    os.remove(hopkins_file)
#    print(hopkins_file+" File Removed!")


#nyt_us_covid19 = requests.get("https://raw.githubusercontent.com//nytimes/covid-19-data/master/us.csv",allow_redirects=True)
#nyt_file = open("tmp/nyt_us.csv","wb+")
#nyt_file.write(nyt_us_covid19.content)

#johns_hopkins_dataset = requests.get("https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv",allow_redirects=True)
#hopkins_file = open("tmp/hopkins.csv","wb+")
#hopkins_file.write(johns_hopkins_dataset.content)


date_nyt = []
cases_nyt = []
deaths_nyt = []
recoveries_nyt = []

date_hopkins = []
recoveries_hopkins = []

joint_list_date = []
joint_list_cases = []
joint_list_deaths = []
joint_list_recoveries = []

country = ""

nyt_data = []
hopkins_data = []

from contextlib import closing
import codecs

url_nyt = "https://raw.githubusercontent.com//nytimes/covid-19-data/master/us.csv"
url_hopkins = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"

with closing(requests.get(url_nyt, stream=True,allow_redirects=True)) as r:
    csvReader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'))

    header_row = next(csvReader)
    #look for Date, Cases and Deaths, this is very ugly, to be redone!!!
    if (header_row[0] == "date" or header_row[0] == "Date"):
        idx_nyt_date = 0;
    elif(header_row[0] == "cases" or header_row[0] == "Cases"):
        idx_nyt_cases = 0;
    elif(header_row[0] == "deaths" or header_row[0] == "Deaths"):
        idx_nyt_deaths = 0;

    if (header_row[1] == "date" or header_row[1] == "Date"):
        idx_nyt_date = 1;
    elif(header_row[1] == "cases" or header_row[1] == "Cases"):
        idx_nyt_cases = 1;
    elif(header_row[1] == "deaths" or header_row[1] == "Deaths"):
        idx_nyt_deaths = 1;

    if (header_row[2] == "date" or header_row[2] == "Date"):
        idx_nyt_date = 2;
    elif(header_row[2] == "cases" or header_row[0] == "Cases"):
        idx_nyt_cases = 2;
    elif(header_row[2] == "deaths" or header_row[0] == "Deaths"):
        idx_nyt_deaths = 2;


    for row in csvReader:
        #We want to skip the header
        if (row[0] == "date" or row[0] == "Date"):
            continue
        #we only want on list which is made of date, cases, deaths
        #in case formatting changes (cases changing row for instance, let's make the program more robust)
        date_nyt.append(pd.to_datetime(row[idx_nyt_date], errors='coerce', infer_datetime_format=True, format='%Y-%m-%d').date())
        cases_nyt.append(row[idx_nyt_cases])
        deaths_nyt.append(row[idx_nyt_deaths])

        #old way, hardcoded values, works
        #date_nyt.append(pd.to_datetime(row[0], errors='coerce', infer_datetime_format=True, format='%Y-%m-%d').date())
        #cases_nyt.append(row[1])
        #deaths_nyt.append(row[2])

        recoveries_nyt.append("0")

idx_hopkins_recoveries = 0



with closing(requests.get(url_hopkins, stream=True,allow_redirects=True)) as r:
    csvReader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'))

    header_row = next(csvReader)
    #look for Date, Cases and Deaths, this is very ugly, to be redone!!!
    if (header_row[0] == "recoveries" or header_row[0] == "Recoveries"):
        idx_hopkins_recoveries = 0;
    if (header_row[1] == "recoveries" or header_row[1] == "Recoveries"):
        idx_hopkins_recoveries = 1;
    if (header_row[2] == "recoveries" or header_row[2] == "Recoveries"):
        idx_hopkins_recoveries = 2;
    if (header_row[3] == "recoveries" or header_row[3] == "Recoveries"):
        idx_hopkins_recoveries = 3;
    if (header_row[4] == "recoveries" or header_row[4] == "Recoveries"):
        idx_hopkins_recoveries = 4;
    if (header_row[5] == "recoveries" or header_row[5] == "Recoveries"):
        idx_hopkins_recoveries = 5;
    if (header_row[6] == "recoveries" or header_row[6] == "Recoveries"):
        idx_hopkins_recoveries = 6;
    if (header_row[7] == "recoveries" or header_row[7] == "Recoveries"):
        idx_hopkins_recoveries = 7; 




    for row in csvReader:
        # Only copy rows if data is for US
        #We want to skip the header
        if (row[0] == "date" or row[0] == "Date"):
            continue
        if row[1] == "US":
            #also only copy row if the date is present in the NYT list!
            hopkins_date_lookup_for_nyt_list = pd.to_datetime(row[0], errors='coerce', infer_datetime_format=True, format='%Y-%m-%d').date()

            for idx,date in enumerate(date_nyt):
                if date == hopkins_date_lookup_for_nyt_list:
                    date_hopkins.append(hopkins_date_lookup_for_nyt_list)
                    recoveries_hopkins.append(row[idx_hopkins_recoveries])
#                    recoveries_hopkins.append(row[6])



# We will now join both lists
#make sure that the date is present in both lists
#if not present, discard and move to next one

for idx_nyt, loop_date_nyt in enumerate(date_nyt):
    #print(data,idx,cases_nyt[idx])
    #now loop through hopkins database
    for idx_hop, loop_date_hop in enumerate(date_hopkins):
        if loop_date_hop == loop_date_nyt:
             
#get date, cases, deaths from NYT, recoveries frop Hopkins
             joint_list_date.append(loop_date_hop)
             joint_list_cases.append(cases_nyt[idx_nyt])
             joint_list_deaths.append(deaths_nyt[idx_nyt])
             joint_list_recoveries.append(recoveries_hopkins[idx_hop])


#print("entries in nyt table",len(date_nyt))
#print("entries in hopkins table",len(date_hopkins))
#print("entries in joint table",len(joint_list_date))


#DynamoDB database should be created by Terraform IAC tool 
def create_covid19_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    try:
        table = dynamodb.create_table(
            TableName="Covid19",
            KeySchema=[
                {
                    'AttributeName': 'Date',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'Date',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        return table
    except dynamodb.exceptions.ResourceInUseException:
        raise

#covid19_table = create_covid19_table()
#covid19_table.wait_until_exists()
#time.sleep(20);
#print("Table status:", covid19_table.table_status)
#print("now let's copy data!!!!")

#Boto3 is built on top of Boto so need to import Boto to get basic functions such as Exceptions
from boto3.dynamodb.conditions import Attr
import botocore


dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
covid19_table = dynamodb.Table('Covid19') 


response = {}
count_added_row = 0

for idx_joint_list, list in enumerate(joint_list_date):
    try:
        response = covid19_table.put_item(
             Item={
                'Date':str(joint_list_date[idx_joint_list]),
                'Cases':joint_list_cases[idx_joint_list],
                'Deaths':joint_list_deaths[idx_joint_list],
                'Recoveries':joint_list_recoveries[idx_joint_list]
            },
            ConditionExpression = Attr("Date").not_exists()
        )
        print(type(response))
        if response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            count_added_row += 1
    except botocore.exceptions.ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up
        # other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise

print("We have added "+ str(count_added_row) + " rows")

import boto3

print("Writing data to database is done, let's send a SNS!!!")
# Create an SNS client
sns = boto3.client('sns','eu-west-3')

# Publish a simple message to the specified SNS topic
response = sns.publish(
    #TopicArn='arn:aws:kms:eu-west-3:888038558695:key/93bcc62a-09e0-4a04-a304-00f496bc4676:covid19_database_update',
    TopicArn='arn:aws:sns:eu-west-3:888038558695:covid19_database_update',    
    Message=("Our program has just added" + str(count_added_row) + " rows to our US Covid19 database"),    
)


#covid19_table.delete()

