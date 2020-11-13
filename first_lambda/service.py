import datetime
import json
import os
import boto3
import pandas as pd
import io
import requests
import numpy as np
from io import StringIO
import uuid



s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2')
bucket_name = 'secom-daas-bucket' # already created on S3

link1 = 'https://archive.ics.uci.edu/ml/machine-learning-databases/secom/secom.data'
link2 = "https://archive.ics.uci.edu/ml/machine-learning-databases/secom/secom_labels.data"
links = [link1,link2]

path = "/tmp/"
timestamp = str(int(datetime.datetime.timestamp(datetime.datetime.now())))

def timestampify(link,timestamp):
    return link.split("/")[-1].split(".")[0]+"_"+timestamp+".data"

data_filename = timestampify(link1,timestamp)
label_filename = timestampify(link2,timestamp)


def download_data():
    
    url = link1
    
    r = requests.get(url)
    with open(path + data_filename, 'wb') as f:
        f.write(r.content)
        files = r.content
        f.close()
    print("Downloaded Secom data.")
    
    url = link2
    r = requests.get(url)
    with open(path + label_filename, 'wb') as f:
        f.write(r.content)
        files = r.content
        f.close()
    print("Downloaded Secom labels.")
    #time_stamp = str(int(datetime.datetime.timestamp(datetime.datetime.now())))

def process_time(secom_labels):
    return [" ".join(i.decode("utf-8").split()[1:]).split('"')[1] for i in secom_labels]

def process_data(secom):
    return np.array([pd.to_numeric(bytearray(i).decode("UTF-8").split(),errors='coerce') for i in secom]).astype(str)

def process_dataset(secom_path,secom_labels_path):

    print("processing dataset from {} and {}".format(secom_path,secom_labels_path))
    #read the downloaded .data files   
    with open(secom_path,'rb') as myfile:
        secom= myfile.readlines() 
        myfile.close()

    with open(secom_labels_path,'rb') as myfile:
        secom_labels= myfile.readlines() 
        myfile.close()

    columns1= ["Time"]
    df1 = pd.DataFrame(data=process_time(secom_labels),
                       columns=columns1)
    df1

    features_size = len(secom[0].split())
    columns2 = ["feature "+ str(i) for i in range(features_size)]
    df2 = pd.DataFrame(data=process_data(secom),
                       columns=columns2)

    df2.fillna(df2.mean(),inplace=True)
    df3 = pd.concat([df1,df2],axis=1).reset_index()

    df3 = df3.rename(columns = {'index':'secomId'})
    #set the secomId as unique ids
    df3['secomId'] = pd.Series([int(uuid.uuid4().int/(10**30)) for i in range(df3.shape[0])])


    return df3




    
#bucket = 'my_bucket_name' # already created on S3
def upload_to_s3(df,bucket_name,dest_path='df.csv'):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    #s3_resource = boto3.resource('s3')
    s3.Object(bucket_name, dest_path).put(Body=csv_buffer.getvalue())
    print("Succesfully stored csv file into S3...")




def handler(event, context):
    # Your code goes here!
    startTime = datetime.datetime.now()
 
    download_data()
    
    df = process_dataset(path + data_filename,path + label_filename)

    upload_to_s3(df, bucket_name, 'processed/processed_'+timestamp+".csv" )
    

    print(datetime.datetime.now() - startTime)




handler(1,1)