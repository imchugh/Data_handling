# -*- coding: utf-8 -*-


import pdb
import os
from datetime import datetime as dt

def cut_raw_file_into_daily_steps():
    
    """
    Created on Wed Feb 18 16:10:05 2015
    
    @author: ian_mchugh
    
    Cuts up large TOA5 into daily chunks and returns data report (% available)
    """
    
    def filename(siteString,dateString):
        dateObj = dt.strptime(dateString,'%Y-%m-%d')
        filenameString = (siteString + '_' + '10Hz' + '_' + dateObj.strftime('%Y')
                          + '_' + dateObj.strftime('%j') + '.txt')
        return filenameString
        
    def datareport(filename,recs_avail):
        pct_avail = str(round(recs_avail / 8640.0,1))
        (logfileObj.writelines(filename + ',' + str(recs_avail) + ','
         + pct_avail + '\n'))
    
    #Specify formats
    dateformatString = '%Y-%m-%d'
        
    #Set file paths
    inpath = 'D:/Data/Raw10Hzdata/Whroo10Hz/temp/'
    infile= 'Whroo_ts_30min.dat'
    outpath = 'R:/Site 10Hz data/Whroo/Out'
    
    #Create log file
    logfile = os.path.join(outpath,'Data_report.txt')
    logfileObj = open(logfile,'w')
    logfileObj.writelines('File, # records found, %records available\n')
    
    # Initialisations
    prevdateString = None
    counter=0
    numrecsCounter=0
    header_lst=[]
    data_lst=[]
    
    f=os.path.join(inpath,infile)
    
    with open(f,'r') as openfileobject:
        for line in openfileobject:
            counter=counter+1
            if counter<5:
                header_lst.append(line)
            else:
                numrecsCounter=numrecsCounter+1
                line=line.split(',')            
                startdateString=line[0][1:11]
                del(line[1])
                line=','.join(line)
                if counter==5:
                   prevdateString=startdateString
                   sub_header=header_lst[1].split(',')
                   del(sub_header[1])
                   header_lst[1]=','.join(sub_header)
                if startdateString==prevdateString:
                    data_lst.append(line)
                else:
                    filenameString=filename('Whroo',startdateString)
                    fileoutObj = open(os.path.join(outpath,filenameString),'w')
                    fileoutObj.writelines(header_lst[1:3])
                    fileoutObj.writelines(data_lst)
                    fileoutObj.close()
                    datareport(filenameString,numrecsCounter)
                    numrecsCounter=0
                    prevdateString=startdateString
                    data_lst=[]
    
    logfileObj.close()

def rewrite_irregular_10Hz():

    """
    Created on Wed Feb 18 16:10:05 2015
    
    @author: ian_mchugh
    
    Takes irregularly time-stamped Campbell Scientific TOA5 files and concatenates 
    the data to whole day blocks, then writes to file with naming convention of 'sitename_10Hz_DOY';
    also produces a data report (which provides a count of data retained for each day) that
    is written to the same user-specified directory
    
    """
    
    # Define functions
    
    def filename(siteString,dateString,modInt):
        dateObj = dt.strptime(dateString,'%Y-%m-%d')
        dateObj = dateObj + timedelta(days=modInt)
        filenameString = (siteString + '_' + '10Hz' + '_' + dateObj.strftime('%Y')
                          + '_' + dateObj.strftime('%j') + '.txt')
        return filenameString
    
    def findnewdate(dataList,startdateString):
        i = 0
        for record in dataList:
            dateString = dataList[i].split(',')[0][1:11]    
            if startdateString != dateString:
                break
            else:
                i = i + 1
        return i
    
    def countmissingdays(prevdateString,startdateString):
        time_delta = (dt.strptime(startdateString,dateformatString) -
                    dt.strptime(prevdateString,dateformatString))
        num_days = time_delta.days - 1
        return num_days
    
    def datareport(filename,recs_avail):
        pct_avail = str(round(recs_avail / 8640.0,1))
        (logfileObj.writelines(filename + ',' + str(recs_avail) + ','
         + pct_avail + '\n'))
    
    #Specify formats
    dateformatString = '%Y-%m-%d'
        
    #Set file paths
    inpath = 'R:/Site 10Hz data/Whroo/Out'
    outpath = 'R:/Site 10Hz data/Whroo/2012'
    dirList = os.listdir(inpath)
    dirList.sort()
    
    #Initialise...
    fileoutObj = None
    prevdateString = None
    numrecsInt = 0
    
    #Create log file
    file = os.path.join(outpath,'Data_report.txt')
    logfileObj = open(file,'w')
    logfileObj.writelines('File, # records found, %records available\n')
    
    for dataFile in dirList:
    
        #Initialise site name
        if fileoutObj == None:
            siteString = dataFile.split('_')[1]
            
        #Get data from file
        file = os.path.join(inpath,dataFile)
        fileObj = open(file,'r')
        dataList = fileObj.readlines()
        fileObj.close()
    
        #Get variable, unit and date strings
        varnamesunitsString = dataList[1:3]
        dataList = dataList[4:] #Remove the header lines
        startdateString = dataList[0].split(',')[0][1:11]
        enddateString = dataList[-1].split(',')[0][1:11]
      
        #Check whether data records in read file are within time bounds of write file
        #If not, then: 1) find the number of missing days and enter in log file
        #              2) close existing file and start new file
        if startdateString != prevdateString:
            if fileoutObj!=None:
                fileoutObj.close()
                datareport(filenameString,numrecsInt)
                numrecsInt=0
                missingString = countmissingdays(prevdateString,startdateString)
                for i in range(missingString):
                    filenameString = filename(siteString,prevdateString,i + 1)
                    datareport(filenameString,numrecsInt)
            prevdateString = startdateString    #Reset previous file date string
            filenameString = filename(siteString,startdateString,0)
            fileoutObj = open(os.path.join(outpath,filenameString),'w')
            fileoutObj.writelines(varnamesunitsString)
    
        #Check whether current data file crosses day
        if startdateString == enddateString:
            dayendInt = len(dataList)
        else:
            dayendInt = findnewdate(dataList,startdateString)
      
        #Write data to file and increment counter
        fileoutObj.writelines(dataList[0:dayendInt])
        numrecsInt = numrecsInt + dayendInt
    
        #If new day data to write, close existing file and write remaining data to new file   
        if dayendInt != len(dataList):
            fileoutObj.close()
            datareport(filenameString,numrecsInt)
            numrecsInt=0
            prevdateString = enddateString
            filenameString = filename(siteString,enddateString,0)
            fileoutObj = open(os.path.join(outpath,filenameString),'w')
            fileoutObj.writelines(varnamesunitsString)
            fileoutObj.writelines(dataList[dayendInt:])
    
        #On last input file, send report data and close output files 
        if dataFile == dirList[-1]:
            datareport(filenameString,numrecsInt)
            fileoutObj.close()
            logfileObj.close()
        
