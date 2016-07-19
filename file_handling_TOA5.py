# -*- coding: utf-8 -*-


import pdb
import os
import calendar
from datetime import datetime as dt

def cut_raw_file_into_daily_steps():
    
    """
    Created on Wed Feb 18 16:10:05 2015
    
    @author: ian_mchugh
    
    Cuts up large TOA5 into daily chunks and returns data report (% available)
    """
    
    def filename(siteString,dateString):
        dateObj = dt.strptime(dateString,'%Y-%m-%d %H:%M')
        filenameString = (siteString + '_' + '10Hz' + '_' + dateObj.strftime('%Y')
                          + '_' + dateObj.strftime('%j') + '_' + dateObj.strftime('%H')
                          + dateObj.strftime('%M') + '.txt')
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
                startdateString=line[0][1:17]
                del(line[1])
                line=','.join(line)
                if counter==5:
                    prevdateString=startdateString
                    sub_header=header_lst[1].split(',')
                    del(sub_header[1])
                    header_lst[1]=','.join(sub_header)
                if startdateString[:10]==prevdateString[:10]:
                    data_lst.append(line)
                else:
                    filenameString=filename('Whroo',prevdateString)
                    fileoutObj = open(os.path.join(outpath,filenameString),'w')
                    fileoutObj.writelines(header_lst[1:3])
                    fileoutObj.writelines(data_lst)
                    fileoutObj.close()
                    datareport(filenameString,numrecsCounter)
                    numrecsCounter=0
                    prevdateString=startdateString
                    data_lst=[]
                    data_lst.append(line)
    
    logfileObj.close()

def kill_dupes(target_dir, target_file):
    
    with open(os.path.join(target_dir, target_file)) as old_fobj:
        this_set = set()
        unique_list = []
        for line in old_fobj:
            if line not in this_set:
                this_set.add(line)
                unique_list.append(line)
    
    split_file_name = os.path.splitext(target_file)
    new_file_name = split_file_name[0] + '_no_dupes' + split_file_name[1]
    with open(os.path.join(target_dir, new_file_name), 'w') as new_fobj:
        new_fobj.writelines(unique_list)

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
        dateObj = dateObj + dt.timedelta(days=modInt)
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

def run_data_report(target_dir, header_len, exp_records):
    
    # Get data and sort
    all_files_list = os.listdir(target_dir)
    data_files_list = [f for f in all_files_list if len(f) == 28]
    data_files_list.sort()
    
    # Check for multiple years and raise exception if so
    site_list = []
    year_list = []
    days_list = []
    for f in data_files_list:
        this_list = f.split('_')
        site_list.append(this_list[0])
        year_list.append(this_list[2])
        days_list.append(int(this_list[3]))
    site_list = list(set(site_list))
    if not len(site_list) == 1:
        raise Exception('Target directory must contain only files from a single ' \
                        'site - aborting!')
    year_list = list(set(year_list))
    if not len(year_list) == 1:
        raise Exception('Target directory must contain only files from a single ' \
                        'year - aborting!')
    
    # Compare number of days to number of expected days
    exp_days = 366 if calendar.isleap(int(year_list[0])) else 365
    exp_days_list = range(1, exp_days + 1)
    diff_list = [d for d in exp_days_list if not d in days_list]
    string_list = []
    for this_day in diff_list:
        this_str = (site_list[0] + '_10Hz_' + year_list[0] + '_' + 
                    str(this_day).zfill(3) + '_0000.txt,0,0.0\n')
        string_list.append(this_str)
    
    # Check number of records and report
    print 'Processing file: '
    for f in data_files_list:
        with open(os.path.join(target_dir, f), 'r') as fileObj:
            for i, line in enumerate(fileObj):
                pass
            i = i - (header_len - 1)
            this_str = (f + ',' + str(i) + ',' + str(round(float(i) / exp_records * 
                        100)) + '\n')
            string_list.append(this_str)
            print '    ' + f
    string_list.sort()
            
    with open(os.path.join(target_dir, 'new_data_report.txt'), 'w') as logfileObj:
        logfileObj.write('File, # records found, % records available\n')
        logfileObj.writelines(string_list)