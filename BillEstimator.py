import sys, csv, requests, json, os, inspect, time, pprint, traceback
from datetime import datetime
from time import strftime, localtime
from openpyxl import Workbook
import argparse, ast, math, queue, threading
import paho.mqtt.client as mqtt

# Get Folder Path
FolderPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# Create a pretty printer object
pp = pprint.PrettyPrinter(indent=4)

##### Constants #####
# Entity Types
TENANT = 'TENANT'
CUSTOMER = 'CUSTOMER'
USER = 'USER'
DASHBOARD = 'DASHBOARD'
ASSET = 'ASSET'
DEVICE = 'DEVICE'
ALARM = 'ALARM'

# Formats
XLSX = 'XLSX'
CSV = 'CSV'

#Aggregation modes
AVG = 'AVG'
MIN = 'MIN'
MAX = 'MAX'
NONE = 'NONE'
SUM = 'SUM'
COUNT = 'COUNT'

#keyList Mode
ALL = "ALL"

#MQTT Broker
BrokerHOST = '35.202.49.101'
MQTTPort = 1883

# Main Function
def main(args):
    try:
        mode = args.mode
        entity_type = args.entity_type
        entity_id = args.entity_id
        isTelemetry = args.isTelemetry
        keyList = args.keyList.split(',')
        price_kwh = args.price_kwh
    except:
        pass

    if(mode == "getToken"):
        getToken()
    elif mode == "getKeyList":
        getKeyList(entity_type, entity_id,isTelemetry)
    elif mode == "getLatestValue":
        getLatestValue(entity_type, entity_id, keyList)
    elif mode == "EstimateBill":
        EstimateBill()
    else:
        raise ValueError("Unimplemented mode")

#Convert Timestamp unix to datetime
def UNIXtoDatetime(unix_ts):
    return datetime.fromtimestamp(unix_ts/1000).strftime("%Y-%m-%d %H:%M:%S")

# Function to get JWT_Token
def getToken():
    url = 'http://35.202.49.101:8080/api/auth/login'
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    loginJSON = {'username': 'tekno@vioint.co.id', 'password': 'vio'}
    tokenAuthResp = requests.post(url, headers=headers, json=loginJSON).json()
    token = tokenAuthResp['token']
    
    #Return token in string format
    return token

# Function to Get All (Arrtibute/Telemetry) Variable Name in Device
def getKeyList(entity_type, entity_id, isTelemetry=True):
    # Args:
    # - entity_type   : DEVICE, ASSET, OR ETC
    # - entity_id     : ID of the entity
    # Return:
    # - KeyList          : List of variable name

    JWT_Token = getToken()
    if isTelemetry:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/keys/timeseries' %(entity_type,entity_id)
    else:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/keys/attributes' %(entity_type,entity_id)
    headers = {'Accept':'application/json', 'X-Authorization': "Bearer "+JWT_Token}
    KeyList = requests.get(url, headers=headers, json=None).json()
    
    return KeyList

# Function to Get Latest Variable Value in Device
def getLatestValue(entity_type, entity_id, isTelemetry=True,keyList=ALL):
    # Args:
    # - entity_type   : DEVICE, ASSET, OR ETC
    # - entity_id     : ID of the entity
    # Return:
    # - LatestValue   : Dictionary of variable names and their latest value

    JWT_Token = getToken()
    if isTelemetry:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/values/timeseries?keys=' %(entity_type,entity_id)
    else:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/values/attributes?keys=' %(entity_type,entity_id)

    if keyList==ALL :
        keys=getKeyList(entity_type, entity_id, isTelemetry)
    else:
        keys=keyList
        
    for i,key in enumerate(keys):
        if i != len(keys)-1:
            url += key + ','
        else:
            url += key + '&'

    headers = {'Accept':'application/json', 'X-Authorization': "Bearer "+JWT_Token}
    LatestValue = requests.get(url, headers=headers, json=None).json()

    #Remove timestamp and extract values
    for key in keys:
        LatestValue[key]=ast.literal_eval(LatestValue[key][0]['value'])
    
    return LatestValue

# Collect Bill Data
def BillDataCollecter(Result, name, entity_type, entity_id, token, isTelemetry, keyList, price_kwh):
    E = list(getLatestValue(entity_type, entity_id, isTelemetry,keyList).values())[0]
    Result.append({"name":name,"Energy":round(E,2),"Bill":int(E*price_kwh)})

    Token = token
    client = mqtt.Client()
    client.username_pw_set(Token)
    client.connect(BrokerHOST, MQTTPort, 60)
    client.publish('v1/devices/me/telemetry', json.dumps({"Bill":int(E*price_kwh)}), 0)
    
# Function to estimate electricity bill
def EstimateBill():
    while(True):
        try:
            with open(FolderPath + "/Config.json", "r") as json_data:
                data = json.load(json_data)
                devices = list(data[0].values())
                price_kwh = list(data[1].values())[0]
                interval = list(data[1].values())[1]
                
            Result = []
            for i,device in enumerate(devices):
                globals().update(device)
                t = threading.Thread(target=BillDataCollecter, args=[Result,name,'DEVICE',entity_id,AccessToken,True,[key],price_kwh])
                t.start()
            
            for i,device in enumerate(devices):
                t.join()

            with open(FolderPath + "/Result.json", "w") as file:
                json.dump(Result,file,indent=4)

            #pp.pprint(Result)
            time.sleep(interval)
        except:
            tb = traceback.format_exc()
            print(tb)
            time.sleep(60)
            pass
       
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, help="Telemetry controller API", default=None)
    parser.add_argument("--entity_type", type=str, help="type of the entity", default=DEVICE)
    parser.add_argument("--entity_id", type=str, help="ID of the entity", default=None)
    parser.add_argument("--keyList", type=str, help="List of variable name", default=None)
    parser.add_argument("--isTelemetry", type=bool, help="1 for telemetry, 0 for attributes", default=1)
    
    args = parser.parse_args(sys.argv[1:]);
    
    main(args);
