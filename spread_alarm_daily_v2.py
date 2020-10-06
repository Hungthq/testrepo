#!/usr/bin/env python3.6
import pandas as pd
import numpy as np
import re
import sys
import os

from collections import defaultdict
import scipy
import scipy.optimize
import numpy
import random
from sklearn.preprocessing import OneHotEncoder

from datetime import datetime

parent_dir = '/home/ehungth/MoAI_Data_Engineer/alarm_spreading/'
#open alarm history file to add sitename_ru & maptime

if not len(sys.argv) > 1:
    print ("Usage:")
    print ("# ./spread.py <full_path_to_file.txt>")
    sys.exit(0)
else:
    inputfile = sys.argv[1]
    #spreaded_alarm = parent_dir + sys.argv[2]

#Create working folders

global path1
global path2

global start_time
global end_time

start_time = '2019-11-01 0000\n'
end_time = '2020-07-31 2345\n'

directory1 = "RULIST/"
path1 = os.path.join(parent_dir, directory1)
#os.mkdir(path1)

directory2 = "ALARMLIST/"
path2 = os.path.join(parent_dir, directory2)
#os.mkdir(path2)


#################

def split_alarm(rucsv):
    #add alarm at start_time & alarm cleared at endtime to fix issue with alarm started before started time and ended after end_time
    #rucsvf = '/mnt/c/temp/ALARMLIST/' + rucsv
    file = open(rucsv, 'r')
    al = []
    for line in file:
        al.append(line)

    if re.search('Cleared', al[1]):
        alarm_insert = al[1].split(",")
        alarm_insert[3] = "minor"
        alarm_insert[-1] = start_time
        al.insert(1, ",".join(alarm_insert))
        print (al)

    if re.search('minor', al[-1]):
        alarm_insert = al[-1].split(",")
        alarm_insert[3] = "Cleared"
        alarm_insert[-1] = end_time
        al.append(",".join(alarm_insert))

    #write every single alarm to file to spread alarm to 15min resolution

    alarm = False
    for line in al:
    #print (line)
        if re.search("minor", line):
            if alarm:
                pass
            else:
                filename = path2 + line.split(',')[0] + "_" + "_".join(line.split(',')[2].split()) + ".csv"
                #filename = "/home/ehungth/MoAI_Data_Engineer/alarm_spreading/ALARMLIST/" + line.split(',')[0] + "_" + "_".join(line.split(',')[2].split()) + ".csv"
                filename = filename.replace(':', '_')
                outalarm = open(filename, "w+")
                outalarm.write ("SITENAME_RU,SITENAME,DATETIME,SEVERITY,SPECIFIC PROBLEM,MANAGED OBJECT,MAPTIME")
                outalarm.write ("\n")
                outalarm.write (line)
                alarm = True
        if re.search("Cleared", line):
            if alarm:
                outalarm.write (line)
                alarm = False
                outalarm.close()


#################

def spread_alarm (alarm, alarm_filename):
    alarm = path2 + alarm
    #alarm = "/home/ehungth/MoAI_Data_Engineer/alarm_spreading/ALARMLIST/" + alarm
    fm = pd.read_csv(alarm, delimiter=",")

    fm['MAPTIME'] = fm['MAPTIME'].astype('datetime64[ns]')


    # map the time to 15 min interval
    rng = pd.date_range(start=fm['MAPTIME'][0], end=fm['MAPTIME'][1], freq='D')
    fm_15 = pd.DataFrame({'MAPTIME': rng})
    fm.sort_values(by=['MAPTIME'], inplace=True)



    fm_interval = pd.merge(fm, fm_15, on='MAPTIME', how='outer')

    fm_interval.sort_values(by=['MAPTIME'], inplace=True)
    fm_interval.fillna(method='ffill', inplace=True)
    fm_interval.isnull().sum().sort_values(ascending=False)
    fm_interval.dropna(inplace=True)
    fm_interval.isnull().sum().sort_values(ascending=False)


    # save the temp file
    tempfile = parent_dir + 'temp.csv' 

    fm_interval.to_csv(tempfile, index=False)
    
    #write spreaded alarm to file
    file = open(alarm_filename, "a")

    
    temp_spreaded_alarm = open (tempfile, "r")

    for line in temp_spreaded_alarm:
        if re.search("2020-0", line) or re.search("2019-1", line):
            file.write(line)
        #file.write("\n")
    file.close()
    os.remove(tempfile)


#################



def spread_single_alarm(alarm_filename):
    fm = pd.read_csv("single_alarm.csv", delimiter=",")

    # Create SITENAME_RU list
    sitelist =[]
    for i in fm['SITENAME_RU']:
        if i not in sitelist:
            sitelist.append(i)
            print (i)
    
    #get alarm list for every hardware:
    
    fm2 = pd.read_csv("single_alarm.csv", index_col='SITENAME_RU')
    
    
    print (path1)
    print (path2)
    for ru in sitelist:
        #outputfile = path1 + ru + ".csv"
        outputfile = "/home/ehungth/MoAI_Data_Engineer/alarm_spreading/RULIST/" + str(ru) + ".csv"
        file =open(outputfile, 'w+')
        rualarm = fm2.loc[ru]
        rualarm.to_csv(outputfile)
        file.close()
    
    
    
    
    for file in os.listdir (path1):
        tempfile = path1 + file
        log = open(tempfile, 'r')
        index = 0
        rm = 0
        for line in log:
            if re.search ('SITENAME', line) and index == 0 :
                log.close()
                break
            else:
                sitename_ru = line.split(",")[-1].strip()
                index += 1
                if re.search("SITENAME", line):
                    sitename = line.split(',')[-1].strip()
                if re.search("DATETIME", line):
                    datetime = line.split(',')[-1].strip()
                if re.search("SEVERITY", line):
                    severity = line.split(',')[-1].strip()
                    if re.search ("Cleared", severity):
                        rm = 1
                    else: 
                        rm = 2
                if re.search("SPECIFIC", line):
                    sp = line.split(',')[-1].strip()
                if re.search("MANAGED", line):
                    mo = line.split(',')[-1].strip()
                if re.search("MAPTIME", line):
                    maptime = line.split(',')[-1].strip()
                    temp_log = sitename_ru + "," + sitename + "," + datetime + "," + severity + "," + sp + "," + mo + "," + maptime + "\n"
                    temp_log1 = sitename_ru + "," + sitename + "," + datetime + "," + severity + "," + sp + "," + mo + "," + start_time 
                    temp_log2 = sitename_ru + "," + sitename + "," + datetime + "," + severity + "," + sp + "," + mo + "," + end_time 
                
                
        if rm == 1:
            log.close()
            os.remove(tempfile)
            log = open(tempfile, "w+")
            log.write ("SITENAME_RU,SITENAME,DATETIME,SEVERITY,SPECIFIC PROBLEM,MANAGED OBJECT,MAPTIME\n")
            log.write (temp_log1)
            log.write (temp_log)
            log.close()
        if rm == 2:
            log.close()
            os.remove(tempfile)
            log = open(tempfile, "w+")
            log.write ("SITENAME_RU,SITENAME,DATETIME,SEVERITY,SPECIFIC PROBLEM,MANAGED OBJECT,MAPTIME\n")
            log.write (temp_log)
            log.write (temp_log2)
            log.close()
    
    
    for ru in sitelist:
        faulty_ru = path1 + ru + ".csv"

        split_alarm(faulty_ru)
    
    #function to spread one single alarm
    
    
    for i in os.listdir(path2):
        spread_alarm (i, alarm_filename)
    
    
    location = '/home/ehungth/MoAI_Data_Engineer/alarm_spreading/ALARMLIST/'
    for file in os.listdir (location):
        path = os.path.join(location, file)
        os.remove(path)
  
    location = '/home/ehungth/MoAI_Data_Engineer/alarm_spreading/RULIST/'
    for file in os.listdir (location):
        path = os.path.join(location, file)
        os.remove(path)

    #path1 = os.path.join(parent_dir, directory)
    #os.rmdir(path1)
    ##directory = "ALARMLIST"
    ##path = os.path.join(parent_dir, directory)
    #os.rmdir(path2)
    


##################
#create spreaded_alarm.csv file to store all the spreaded alarm
ru_alarm_filename = parent_dir + "RU_" + sys.argv[2]
file = open(ru_alarm_filename, "w+")
file.write("SITENAME_RU,SITENAME,DATETIME,SEVERITY,SPECIFIC PROBLEM,MANAGED OBJECT,MAPTIME")
file.write("\n")
file.close()

du_alarm_filename = parent_dir + "DU_" + sys.argv[2]
file = open(du_alarm_filename, "w+")
file.write("SITENAME_RU,SITENAME,DATETIME,SEVERITY,SPECIFIC PROBLEM,MANAGED OBJECT,MAPTIME")
file.write("\n")
file.close()


cell_alarm_filename = parent_dir + "CELL_" + sys.argv[2]
file = open(cell_alarm_filename, "w+")
file.write("SITENAME_RU,SITENAME,DATETIME,SEVERITY,SPECIFIC PROBLEM,MANAGED OBJECT,MAPTIME")
file.write("\n")
file.close()
##################

# Spreading RU level Alarms

########################

ru_level_alarm = ["Link Failure","Link Degraded","HW Fault","Linearization Disturbance Performance Degraded","SW Error","Current Too High","No Connection","Power Loss","VSWR Over Threshold","RF Reflected Power High"]

#NodeName,Nodename_Rilink,RU,NodeName_RU,RiLink
#PRI0013-L21,PRI0013-L21_1,RRU-1,PRI0013-L21_RRU-1,RiLink=1
#PRI0013-L21,PRI0013-L21_2,RRU-2,PRI0013-L21_RRU-2,RiLink=2
#PRI0013-L21,PRI0013-L21_3,RRU-3,PRI0013-L21_RRU-3,RiLink=3
#PRI0013-L23,PRI0013-L23_1,RRU-1,PRI0013-L23_RRU-1,RiLink=1
#PRI0013-L23,PRI0013-L23_2,RRU-2,PRI0013-L23_RRU-2,RiLink=2
rilink_data = open ("Rilink.csv")

rilink ={}
for line in rilink_data:
    if re.search("RiLink", line):
        rilink [line.split(",")[1]] = line.split(",")[3]
        
#############

for alarm in ru_level_alarm:
    log =open(inputfile, 'r')
    single_alarm_temp_file = open("single_alarm.csv", "w+")
    for line in log:
        if re.search("SITENAME", line):
            single_alarm_temp_file.write (line)
        elif re.search ("RU-", line) :
            if re.search(alarm, line):
                single_alarm_temp_file.write (line)
        elif re.search ("RiLink", line):
            #SITENAME_RU,SITENAME,DATETIME,SEVERITY,SPECIFIC PROBLEM,MANAGED OBJECT,MAPTIME
            #BKK0124-L23_5,BKK0124-L23,2020-05-08 11:45:23,minor,Link Failure,RiLink=5,2020-05-08 11:45:00
            #BKK0124-L23_5,BKK0124-L23,2020-05-08 12:06:21,Cleared,Link Failure,RiLink=5,2020-05-08 12:00:00
            if re.search(alarm, line):
                temp = line.split(",")
                if temp[5] in rilink.keys():
                    temp[5] = rilink[temp[5]]
                    line = ",".join(temp)
                    single_alarm_temp_file.write (line)
    single_alarm_temp_file.close()
    spread_single_alarm (ru_alarm_filename)
    os.remove("single_alarm.csv")



    
du_level_alarm = ["Aggregated Ethernet Link Failure","Certificate Management a Valid Certificate is Not Available","Clock Calibration Expiry Soon","Critical Temperature Performance Degraded","Critical Temperature Taken Out of Service","Ethernet Link Failure","Fan Speed Continuously High","Fan Failure","File System Diagnostic Error","General SW Error","Gigabit Ethernet Link Fault","Invalid Ethernet Optical Module","LACP Failure","License Key File Fault","License Key Not Available","Network Synch Time from GPS Missing","NTP System Time Sync Fault","Plug-In Unit General Problem","Remote IP Address Unreachable","Resource Configuration Failure","SFP HW Fault","SFP Not Present","SW Download Failure","Sync PTP Time Availability Fault","Sync PTP Time PDV Problem","Sync PTP Time Reachability Fault","Sync PTP Time Reliability Fault","System Clock Quality Degradation","Temperature Exceptional Taken Out of Service","Temperature Abnormal","Timing Sync Fault","TU Hardware Fault"]

for alarm in du_level_alarm:
    log =open(inputfile, 'r')
    single_alarm_temp_file = open("single_alarm.csv", "w+")
    for line in log:
        if re.search("SITENAME", line):
            single_alarm_temp_file.write (line)
        if re.search(alarm, line):
            single_alarm_temp_file.write (line)
    single_alarm_temp_file.close()
    spread_single_alarm (du_alarm_filename)
    os.remove("single_alarm.csv")

cell_level_alarm = ["Service Degraded","Service Unavailable","Suspected Sleeping Cell"]

for alarm in cell_level_alarm:
    log =open(inputfile, 'r')
    single_alarm_temp_file = open("single_alarm.csv", "w+")
    for line in log:
        if re.search("SITENAME", line):
            single_alarm_temp_file.write (line)
        elif re.search ("EUtranCell", line):
            if re.search(alarm, line):
                single_alarm_temp_file.write (line)
    single_alarm_temp_file.close()
    spread_single_alarm (cell_alarm_filename)
    os.remove("single_alarm.csv")

#Structure alarm:
##############



def structure_alarm(alarm, structure_alarm):
    df=pd.read_csv(alarm)
    enc = OneHotEncoder(handle_unknown='ignore')
    enc_df = pd.DataFrame(enc.fit_transform(df[['SPECIFIC PROBLEM']]).toarray())
    enc_df.columns = enc.get_feature_names()
    enc_df.head()
    col=enc_df.columns
    enc_df['SITENAME']=df['SITENAME_RU']
    enc_df['MAPTIME']=df['MAPTIME']
    
    df_grp=enc_df.groupby(['SITENAME','MAPTIME']).sum().reset_index(drop=False)
    
    for i in col:       
        df_grp[i]=[1 if x>0 else 0 for x in df_grp[i]]

    df_grp.to_csv(structure_alarm)

def structure_alarm_du(alarm, structure_alarm):
    df=pd.read_csv(alarm)
    enc = OneHotEncoder(handle_unknown='ignore')
    enc_df = pd.DataFrame(enc.fit_transform(df[['SPECIFIC PROBLEM']]).toarray())
    enc_df.columns = enc.get_feature_names()
    enc_df.head()
    col=enc_df.columns
    enc_df['SITENAME']=df['SITENAME']
    enc_df['MAPTIME']=df['MAPTIME']

    df_grp=enc_df.groupby(['SITENAME','MAPTIME']).sum().reset_index(drop=False)

    for i in col:
        df_grp[i]=[1 if x>0 else 0 for x in df_grp[i]]

    df_grp.to_csv(structure_alarm)

structured_du_alarm_filename = parent_dir + "Structured_DU_" + sys.argv[2]
structured_ru_alarm_filename = parent_dir + "Structured_RU_" + sys.argv[2]
structured_cell_alarm_filename = parent_dir + "Structured_CELL_" + sys.argv[2]

structure_alarm (ru_alarm_filename, structured_ru_alarm_filename)
structure_alarm_du (du_alarm_filename, structured_du_alarm_filename)
structure_alarm (cell_alarm_filename, structured_cell_alarm_filename)


