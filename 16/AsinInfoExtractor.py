# -*- coding: utf-8 -*-
import requests
import re, csv
import ast
import time
import html
import sys
import ssl
import subprocess
from random import randint
from random import uniform
import datetime
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta, date
from six.moves.html_parser import HTMLParser
from random import randint
import pandas as pd
import os
import imp
import threading,queue
import ftfy,ftplib
import redis,queue
import configparser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import elasticsearch
import shutil


dir = os.path.dirname(os.path.abspath(__file__))
AutomationController = imp.load_source('AutomationController', dir+'/AutomationController.py')

from AutomationController import Start_Process
from AutomationController import create_config
from AutomationController import DB_update
from AutomationController import Log_writer
from AutomationController import Progress_Count
from AutomationController import email_sender

config = configparser.ConfigParser()
config.read('Config.ini')


r = redis.StrictRedis()
'''
Possible Region: US, CA, CN, DE,ES, FR, JP, IT, UK,IN
'''


Asin_Index="asininfo_cache"
head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]
try:
   date=re.findall(r"Catalog\-([\d]+)\_",str(base))[0]
except:
   date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

doc_type=cNumber+"_"+date

AsinInfoStarted=config.get("Status", "AsinInfo-Started")
AsinInfoFailed=config.get("Status", "AsinInfo-Failed")
AsinInfoCompleted=config.get("Status", "AsinInfo-Completed")

host=config.get("Elastic-Search","host")
port=config.get("Elastic-Search","port")

es = Elasticsearch([{'host': host, 'port': port}])

asin_args=sys.argv[4].replace("[","").replace("]","")
asin_args=re.sub(r"\s+","",asin_args,re.I)
asin_args=re.sub(r"\s+","",asin_args,re.I).split(",")

r.set(str(cNumber)+"_"+str(date)+"_1_progress", 0)
r.set(str(cNumber) + "_" + str(date) + "_1_total", 0)
r.set(str(cNumber) + "_" + str(date) + "_1_total_MissedID", 0)
r.set(str(cNumber) + "_" + str(date) + "_1_progress_MissedID", 0)

h = HTMLParser()

config = configparser.ConfigParser()
config.read('Config.ini')

#FTP
server=config.get("FTP", "server")
username=config.get("FTP", "username")
password=config.get("FTP", "password")
directory=config.get("FTP", "directory")

ftp= ftplib.FTP(server)
ftp.login(username, password)

now = datetime.now()
date_folder=now.strftime("%d_%m_%Y")
ftp_dir=directory+"/"+date_folder+"/"+cNumber
try:
    ftp.cwd(ftp_dir)
except Exception as e:
    print(e)
    try:
        ftp_dir=directory+"/"+date_folder+"/"
        ftp.cwd(ftp_dir)
    except:
        ftp.mkd(ftp_dir)
    ftp_dir=directory+"/"+date_folder+"/"+cNumber	
    ftp.mkd(ftp_dir)
    ftp.cwd(ftp_dir)

def reindex_function(old_index,reindexing_index):
    try:
        data_point = {
            "source": {
                "index": old_index
            },
            "dest": {
                "index": reindexing_index
            }
        }
        print(str(data_point).replace("'", "\""))

        reindex = requests.post("http://" + str(host) + ":" + str(port) + "/_reindex",data=str(data_point).replace("'", "\""))
        print(reindex.status_code == 200)
        if reindex.status_code == 200:
            es.indices.delete(index=old_index)
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"Index Health is RED."+str(old_index)+" deleted... :  ","26", "Index Health is RED."+str(old_index)+" deleted...")
            print(str(old_index)+" index deleted.......")
    except Exception as e:
        print(e)
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,str(e)+" :  " + str(sys.exc_info()[-1].tb_lineno), "28","Issue in ReIndexing...")
        sys.exit(0)

def asin_req(data,country):
    try:
        dt = str(data).replace("[", "").replace("]", "").replace("'", "\"").replace(" ", "")
        retailer_ids = re.sub(r"\s*", "", dt, re.I).replace("\"\"", "\"")
        retailer_ids = re.sub(r"^\"", "", retailer_ids, re.I)
        retailer_ids = re.sub(r"\"$", "", retailer_ids, re.I)
        #print(retailer_ids)
        print(country)
        post_content = {"asins": [retailer_ids], "tld": str(country).lower(),"api-key": "21df0e51-ef9e-4823-a711-30305ea4c6e6"}
        # print(str(post_content).replace("'", "\""))
        header = {
            "content-type": "application/json"
        }

        start_time = datetime.now().time().strftime('%H:%M:%S')
        time.sleep(3)
        res_data = requests.post("https://fw5jam5ycc.execute-api.us-west-2.amazonaws.com/dev/keepa",data=str(post_content).replace("'", "\""), headers=header)
        # print(type(res_data))
        end_time = datetime.now().time().strftime('%H:%M:%S')
        total_time = (datetime.strptime(end_time, '%H:%M:%S') - datetime.strptime(start_time, '%H:%M:%S'))
        print(res_data.status_code)
        print(total_time)
        return res_data,total_time
    except Exception as e:
        print(e)
        if re.findall(r"Max\s*retries\s*exceeded", str(e),re.I):
            time.sleep(300)
            Log_writer("OCR_ErrorLog_" + str(cNumber) + ".log", cNumber,str(e), "444", "Max retry exceeded")
            return "",300
        # Log_writer("OCR_ErrorLog_" + str(cNumber) + ".log", cNumber,str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno), str(AsinInfoFailed), "AsinInfo Failed")
        # time.sleep(500)
        #sys.exit(0)

def Asinator_ping(df,region,spliteddata,outputFile,flag=False):
    try:
        res_retailer_ids = []
        records=[]
        total_retailer_ids = [x for sublist in spliteddata for x in sublist]
        total_retailer_ids = list(filter(None, total_retailer_ids))
        if not os.path.exists(dir + "/temp/"):
            os.mkdir(dir + "/temp/")
        if not os.path.exists(dir + "/temp/" + date_folder):
            os.mkdir(dir + "/temp/" + date_folder)
        if not os.path.exists(dir + "/temp/" + date_folder + "/" + cNumber + "/"):
            os.mkdir(dir + "/temp/" + date_folder + "/" + cNumber)

        r.incrby(str(cNumber) + "_" + str(date) + "_1_total", len(total_retailer_ids))
        for inner_data in spliteddata:
            res_data,total_time=asin_req(inner_data, region)
            if res_data=="":
               continue
            if flag:
                if res_data.status_code==502:
                    time.sleep(300)
                    retry=0
                    res_data, total_time = asin_req(inner_data, region)
                    if res_data=="":
                        continue
                    while res_data.status_code==502 and retry < 2:
                        time.sleep(300)
                        res_data, total_time = asin_req(inner_data, region)
                        if res_data=="":
                           continue
                        retry += 1
                    if retry==2 and res_data.status_code==502:
                        continue
                    else:
                        pass
            if res_data.status_code==200:
                res_data_json = res_data.json()
                for index,data in enumerate(res_data_json):
                    res_retailer_ids.append(data["asin"])
                    try:
                        orgTitle = df["Title"][data["asin"]]
                    except Exception as e:
                        print(e)
                    docAsin = {}
                    NoneFlag=True
                    if data["title"] is None:
                        df.loc[data["asin"],'ProductType']=str(data["productType"])
                        df.loc[data["asin"],"Title"]=str(orgTitle)
                        docAsin["Title"]=str(orgTitle)
                        if data["manufacturer"] == "none" or data["manufacturer"] == "None" or data["manufacturer"] is None:
                            manufacturer = ""
                        else:
                            manufacturer = str(data["manufacturer"]).replace('&AMP;', '&')
                        df.loc[data["asin"], 'Manufacturer'] = manufacturer
                        docAsin['Manufacturer'] = manufacturer
                        if data["brand"] == "none" or data["brand"] == "None" or data["brand"] is None:
                            brand = ""
                        else:
                            brand = str(data["brand"]).replace('&AMP;', '&')
                        df.loc[data["asin"], 'Brand'] = brand
                        docAsin['Brand'] = brand
                        NoneFlag=False
                    if data["asin"]=="":
                        continue
                    try:
                        with open(dir + "/temp/" + date_folder + "/" + cNumber + "/" + str(data["asin"]).replace(",","_") + ".json","w") as t:
                            t.write(str(str(data)))
                        if NoneFlag:
                            # print(asin_args)
                            orgTitle = df["Title"][data["asin"]]
                            if "Asin-Title" in asin_args:
                                df.loc[data["asin"], 'Title'] = str(data["title"]).replace('&AMP;', '&')
                                docAsin['Title'] = str(data["title"]).replace('&AMP;', '&')
                            fuzzyScore = fuzz.token_set_ratio(str(data["title"]), orgTitle)
                            if float(fuzzyScore) < 70:
                                df.loc[data["asin"], 'TitlMatchScore'] = str(fuzzyScore)
                                docAsin['TitlMatchScore'] = str(fuzzyScore)
                            if 'Platform' in df.columns:
                                productGroup = str(data["productGroup"]).replace('&AMP;', '&')
                                if "pantry" == productGroup.lower():
                                    df.loc[data["asin"], 'Platform'] = "PANTRY"
                                    docAsin['Platform'] = "PANTRY"
                                else:
                                    df.loc[data["asin"], 'Platform'] = "DOT COM"
                                    docAsin['Platform'] = "DOT COM"
                            if "Asin-M" in asin_args:
                                if data["manufacturer"] =="none" or data["manufacturer"] =="None" or data["manufacturer"] is None:
                                    manufacturer=""
                                else:
                                    manufacturer=str(data["manufacturer"]).replace('&AMP;', '&')

                                df.loc[data["asin"], 'Manufacturer'] = manufacturer
                                docAsin['Manufacturer'] = manufacturer

                            if "Asin-B" in asin_args:
                                if data["brand"] =="none" or data["brand"] =="None" or data["brand"] is None:
                                    brand=""
                                else:
                                    brand=str(data["brand"]).replace('&AMP;', '&')
                                df.loc[data["asin"], 'Brand'] = brand
                                docAsin['Brand'] = brand

                            if "Asin-P" in asin_args:
                                df.loc[data["asin"], 'ProductGroup'] = str(data["productGroup"]).replace('&AMP;', '&')
                                docAsin['ProductGroup'] = str(data["productGroup"]).replace('&AMP;', '&')

                            if "Asin-IDM" in asin_args:
                                df.loc[data["asin"], 'Identifiers Model'] = str(data["model"])
                                docAsin['Identifiers Model'] = str(data["model"])
                            try:
                                 df.loc[data["asin"], 'BreadCrumb'] = str(data["category"])
                                 docAsin['BreadCrumb'] = str(data["category"])
                            except Exception as e:
                                df.loc[data["asin"], 'BreadCrumb'] = ""
                            try:
                                df.loc[data["asin"],'Images']=data["imagesCSV"]
                                docAsin['Images'] = str(data["imagesCSV"])
                            except Exception as e :
                                df.loc[data["asin"],'Images']=""
                            try:
                                constr = ""
                                for val in data["categoryTree"]:
                                    constr += val["name"] + "|"
                                constr_output = re.sub(r"\|$", "", constr, re.I)
                                df.loc[data["asin"], 'CategoryTree'] = constr_output
                                docAsin['CategoryTree'] = str(constr_output)
                            except Exception as e:
                                df.loc[data["asin"], 'CategoryTree'] = ""

                        insertFlag = False
                        record = {
                            "_op_type": "create",
                            "_index": Asin_Index,
                            "_type": doc_type,
                            "_id": str(data["asin"]).strip(),
                            "_source": docAsin
                        }
                        records.append(record)
                        if len(res_data_json) - 1 == index:
                            print("Flag arised")
                            insertFlag = True
                        if insertFlag:
                            try:
                                print(helpers.bulk(es, records))
                                records = []
                            except helpers.BulkIndexError as e:
                                print(e)
                                Log_writer("OCR_ErrorLog_" + str(cNumber) + ".log", cNumber,str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno), str(AsinInfoFailed),"AsinInfo Failed")
                            except Exception as e:
                                print(e)
                                if es.indices.exists(index="asininfo_cache"):
                                    try:
                                        setting = requests.get("http://" + str(host) + ":" + str(port) + "/_cluster/health/" + str(Asin_Index) + "?level=shards&pretty").json()
                                        print(setting)
                                        if setting['indices'][Asin_Index]["status"] == 'red':
                                            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,str(Asin_Index) + " Index Health is RED.", str(AsinInfoFailed),str(Asin_Index) + " Index Health is RED.")
                                            with open("index_red_health.txt", "a") as fh:
                                                fh.write(str(Asin_Index) + "=====>" + str(cNumber) + "  ======>   " + str(setting['indices'][Asin_Index]["status"]) + "\n")
                                            reindexing_index = str(Asin_Index) + "_reindex"
                                            reindex_function(Asin_Index, reindexing_index)
                                            reindex_function(reindexing_index, Asin_Index)
                                    except Exception as e:
                                        print(e)
                                        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,str(e) + " : " + str(sys.exc_info()[-1].tb_lineno),str(AsinInfoFailed), str(Asin_Index) + " Index Health is RED.")
                                        sys.exit(0)
                                    print(Asin_Index)
                                Log_writer("OCR_ErrorLog_" + str(cNumber) + ".log", cNumber,str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno), str(AsinInfoFailed),"AsinInfo Failed")
                    except Exception as e:
                        print(e)
                        print(sys.exc_info()[-1].tb_lineno)
                        pass
                    outputFile.write(str(data["asin"]) + "\t" + str(data["title"]) + "\t" + str(data["manufacturer"]) + "\t" + str(data["productGroup"]) + "\t" + str(data["brand"]) + "\t" + str(data["category"]) + "\t" + str(data["imagesCSV"]) + "\n")

                with open("log_test.txt", "a") as fh:
                    fh.write(str(cNumber)+" - "+"Status Code : " + str(res_data.status_code) + "  ======> Count : " + str(len(res_data_json)) + " ======> Elapsed Time : " + str(total_time) + "\n")
                print("Processed : "+str(len(res_retailer_ids))+"  : Remaining :"+str(len(list(set(total_retailer_ids)-set(res_retailer_ids)))))
            else:
                print(res_data.status_code)
                with open("log_test.txt", "a",encoding='utf-8') as fh:
                    fh.write(str(cNumber)+" - "+"Status Code : " + str(res_data.status_code) + "  ======  :  ==== Count : " + str(res_data.content) + " ======> Elapsed Time : " + str(total_time) + "\n")
                pass
            r.incrby(str(cNumber) + "_" + str(date) + "_1_progress", len(inner_data))
        # print(res_retailer_ids, set(total_retailer_ids) - set(res_retailer_ids))
        return res_retailer_ids, set(total_retailer_ids) - set(res_retailer_ids)
    except Exception as e:
        print(e)
        print(sys.exc_info()[-1].tb_lineno)
        sys.exit(0)

def datasplit(arr, size):
    arrs = []
    while len(arr) > size:
        pice = arr[:size]
        arrs.append(pice)
        arr   = arr[size:]
    arrs.append(arr)
    return arrs

def input_values(sourcecontent,titleIndex,trackItemIndex,idIndex,IsFulltitleCase):
    max_row = len(sourcecontent)
    id, Ytitle, dataValue = [], [], []
    for row_number in range(1, int(max_row)):
        if (IsFulltitleCase):
            # print("Full title run case")
            dataValue.append(sourcecontent[row_number][idIndex])
        else:
            #print("Needs to review count")
            if 'needs review' in str(sourcecontent[row_number][trackItemIndex]).lower():
                dataValue.append(sourcecontent[row_number][idIndex])
            elif 'needs review' not in str(sourcecontent[row_number][1]).lower():
                id.append(str(sourcecontent[row_number][idIndex]).strip())
                Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())
    return id, Ytitle,dataValue

#Main
if __name__ == "__main__":
    start_time = datetime.now().time().strftime('%H:%M:%S')
    startTime = datetime.now()
    region = sys.argv[2]
    fileName =  str(sys.argv[1])
    IsFulltitleCase=sys.argv[3]
    IsFulltitleCase=ast.literal_eval(IsFulltitleCase)
    df = pd.read_csv(fileName, encoding='latin1')
    inputHeader = (list(df.columns.values))
    titleIndex=inputHeader.index("Title")
    trackItemIndex=inputHeader.index("Track Item")
    idIndex=''
    try:
        idIndex=inputHeader.index("Retailer Item ID")
        df = df.set_index("Retailer Item ID")
    except:
        idIndex=inputHeader.index("Retailer Item ID1")
        df = df.set_index("Retailer Item ID1")
    df.head()
    df['title_old'] = df['Title']
    df['ProductGroup'] = ''
    df['TitlMatchScore'] = ''
    df['BreadCrumb'] = ''
    df['CategoryTree'] = ''
    df['ProductType'] = ''
    df['Images']=''
    outputfilecsv = fileName.replace('.csv', '_p0.csv')
    outputfile = fileName.replace('.csv', '_p0.txt')
    f = open(fileName, 'rt', encoding="ISO-8859-1")
    sourcecontent = []
    if es.indices.exists(index="asininfo_cache"):
        try:
            setting = requests.get("http://" + str(host) + ":" + str(port) + "/_cluster/health/" + str(Asin_Index) + "?level=shards&pretty").json()
            print(setting)
            if setting['indices'][Asin_Index]["status"] == 'red':
                Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(Asin_Index) + " Index Health is RED.",str(AsinInfoFailed), str(Asin_Index) + " Index Health is RED.")
                with open("index_red_health.txt", "a") as fh:
                    fh.write(str(Asin_Index) + "=====>" + str(cNumber) + "  ======>   " + str(setting['indices'][Asin_Index]["status"]) + "\n")
                reindexing_index = str(Asin_Index) + "_reindex"
                reindex_function(Asin_Index, reindexing_index)
                reindex_function(reindexing_index, Asin_Index)
        except Exception as e:
            print(e)
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " : " + str(sys.exc_info()[-1].tb_lineno),str(AsinInfoFailed), str(Asin_Index) + " Index Health is RED.")
            sys.exit(0)
        print(Asin_Index)

    reader = csv.reader(f, delimiter=',')
    for data in reader:
        sourcecontent.append(data)
    outputFile = open(outputfile, "a")
    outputFile.write("retailerID" + "\t" + "title" + "\t" + "manufacture" + "\t" + "pgroup" + "\t" + "Brand" + "\n")
    count = 0
    retailerID = ''
    id, Ytitle, inputData_temp = input_values(sourcecontent,titleIndex,trackItemIndex,idIndex,IsFulltitleCase)
    missingID = []
    existing_id_list = []
    #implementing proper schema using mapping for every catalogs
    dic = {}
    dic["mappings"] = {}
    dic["mappings"][str(cNumber)] = {}
    dic["mappings"][str(cNumber)]["properties"] = {}
    for val in inputHeader:
        dic["mappings"][str(cNumber)]["properties"][val] = {}
        dic["mappings"][str(cNumber)]["properties"][val]["type"] = "string"
    dic["settings"]={}
    dic["settings"]["number_of_replicas"]=0
    setting = requests.put("http://" + str(host) + ":" + str(port) + "/"+str(Asin_Index)+"/", data=dic,headers={"Content-Type": "application/json"})
    #refresh index for every catalogs
    es.indices.refresh(Asin_Index)
    url_1 = "http://" + str(host) + ":" + str(port) + "/"+str(Asin_Index)+"/" + str(doc_type) + "/_search"
    print(url_1)
    ping1 = requests.get(url_1).json()
    window_flag=False
    try:
        window_size = {
            "max_result_window": int(ping1['hits']['total'])+10000
        }
        print(window_size)
        setting = requests.put("http://" + str(host) + ":" + str(port) + "/"+str(Asin_Index)+"/_settings",data=window_size, headers={"Content-Type": "application/json"})
        print(setting.content)
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"Asin Info Cache Window size was increased : " + str(setting.content), "14","Asin Info Cache Window size was increased")
        window_flag=True
    except :
        pass

    url_1 = "http://" + str(host) + ":" + str(port) + "/"+str(Asin_Index)+"/" + str(doc_type) + "/_search"

    ping1 = requests.get(url_1).json()
    try:
        url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
        ping2 = requests.get(url_2).json()
        for val in ping2['hits']['hits']:
            existing_id_list.append(val['_id'])
            if 'Title' in val['_source']:
                df.loc[val['_id'], 'Title'] = val['_source']['Title']
            else:
                df.loc[val['_id'], 'Title'] = ""

            if 'TitlMatchScore' in val['_source']:
                df.loc[val['_id'], 'TitlMatchScore'] = val['_source']['TitlMatchScore']
            else:
                df.loc[val['_id'], 'TitlMatchScore'] = ""

            if 'Brand' in val['_source']:
                df.loc[val['_id'], 'Brand'] = val['_source']['Brand']
            else:
                df.loc[val['_id'], 'Brand'] = ""

            if 'Manufacturer' in val['_source']:
                df.loc[val['_id'], 'Manufacturer'] = val['_source']['Manufacturer']
            else:
                df.loc[val['_id'], 'Manufacturer'] = ""

            if 'Platform' in val['_source']:
                df.loc[val['_id'], 'Platform'] = val['_source']['Platform']
            else:
                df.loc[val['_id'], 'Platform'] = ""

            if 'ProductGroup' in val['_source']:
                df.loc[val['_id'], 'ProductGroup'] = val['_source']['ProductGroup']
            else:
                df.loc[val['_id'], 'ProductGroup'] = ""

            if 'Identifiers Model' in val['_source']:
                df.loc[val['_id'], 'Identifiers Model'] = val['_source']['Identifiers Model']
            else:
                df.loc[val['_id'], 'Identifiers Model'] = ""

            if 'BreadCrumb' in val['_source']:
                df.loc[val['_id'], 'BreadCrumb'] = val['_source']['BreadCrumb']
            else:
                df.loc[val['_id'], 'BreadCrumb'] = ""

            if 'Images' in val['_source']:
                df.loc[val['_id'], 'Images'] = val['_source']['Images']
            else:
                df.loc[val['_id'], 'Images'] = ""

            if 'CategoryTree' in val['_source']:
                df.loc[val['_id'], 'CategoryTree'] = val['_source']['CategoryTree']
            else:
                df.loc[val['_id'], 'CategoryTree'] = ""

    except Exception as e:
        print(e)
    es.indices.refresh(Asin_Index)
    if window_flag:
        window_size = {
            "max_result_window": 10000
        }
        setting = requests.put("http://" + str(host) + ":" + str(port) + "/" + str(Asin_Index) + "/_settings",data=window_size, headers={"Content-Type": "application/json"})
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"Asin_Index Window size was reduced: " + str(setting.content), "15","Asin_Index Window size was reduced")

    inputData = list(set(inputData_temp).difference(existing_id_list))
    inputData=list(filter(None, inputData))
    print("Single Ping")
    record_len=len(inputData)
    spliteddata = datasplit(inputData, 300)
    success_ids,failed_ids=Asinator_ping(df,region,spliteddata,outputFile)
    retry=0
    success_fail_len=len(failed_ids)
    print("success_fail_len"+str(success_fail_len))
    if success_fail_len!=0:
        spliteddata = datasplit(list(failed_ids), 300)
        success_ids, failed_ids = Asinator_ping(df, region, spliteddata, outputFile,True)
        outputFile.close()
        df["Failed IDs"] = ""
        for val in list(failed_ids):
            df.loc[val, 'Failed IDs'] = "Failed Product"
    inputHeader = (list(df.columns.values))
    for item in inputHeader:
        if re.match("Unnamed\:.*?", item, flags=re.I):
            unamedword = re.findall(r'(Unnamed\:.*)', item, flags=re.IGNORECASE)[0]
            unamedIndex = inputHeader.index(unamedword)
            df = df.drop(df.columns[[unamedIndex]], axis=1)
    try:
        df.to_csv(outputfilecsv, sep=',', encoding='latin1')
    except:
        df.to_csv(outputfilecsv, sep=',', encoding='utf-8')
    print ("File Name" + str(fileName) + "Start Time::" + str(startTime) + "\nEnd Time" + str(datetime.now()))
    end_time = datetime.now().time().strftime('%H:%M:%S')
    total_time_1 = (datetime.strptime(end_time, '%H:%M:%S') - datetime.strptime(start_time, '%H:%M:%S'))
    ftp_flag=True
    TEXT = "Hello everyone,\n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Catalog " + str(cNumber) + " Asinator Extraction is processed successfully \nTotal Duration Taken      : "+str(total_time_1)+"\nRecords taken to process  : "+str(record_len)+"\n"+"Failed after success Count  : "+str(success_fail_len)+"\n"+"Failed after retry Count     : "+str(len(failed_ids))+"\n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Thanks,\n"
    TEXT = TEXT + "OCR IT Team"
    email_sender(TEXT,"AsinInfo for Catalog "+str(cNumber))
    Log_writer("OCR_generic_error.log", str(cNumber), "AsinInfo Completed", str(AsinInfoCompleted), "AsinInfoCompleted")
    try:
        if os.path.exists(dir+"/temp/"+date_folder+"/"+cNumber+"/"):
            for root, dirs, files in os.walk(dir + "/temp/" + str(date_folder) + "/" + str(cNumber) + "/"):
                for fname in files:
                    full_fname = os.path.join(root, fname)
                    ftp.storbinary('STOR ' + str(ftp_dir)+"/" + str(fname), open(full_fname, 'rb'))
        else:
            Log_writer("OCR_ErrorLog_" + str(cNumber) + ".log", cNumber, "Mentioned dir not exists :  ", str(AsinInfoFailed), "AsinInfo Failed")
    except Exception as e:
        ftp_flag=False
        #Log_writer("OCR_ErrorLog_" + str(cNumber) + ".log", cNumber, str(e)+" :  "+str(sys.exc_info()[-1].tb_lineno), str(AsinInfoFailed),"AsinInfo Failed")

    if ftp_flag:
        shutil.rmtree(dir+"/temp/"+str(date_folder)+"/"+str(cNumber)+"/" )

    # es.indices.delete(index='asininfo_cache')
    if len(sys.argv)==5:
        if sys.argv[4] =="start-single":
            print("Asin Info process Success")
            # sys.exit(0)
        else:
            e="Wrong Argument,need start-single as 3 rd argument"
            # print(e, sys.exc_info()[-1].tb_lineno)
            # Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-2", "Asin Info process is Failed")

    elif len(sys.argv) == 4:
        print("Asin Info process Success")
        create_config(True,sys.argv[1],cNumber)
        data_parse="ProductClassifier.py "+outputfilecsv
        Start_Process(data_parse)

