# -*- coding: utf-8 -*-


import pdb
import os
from datetime import datetime as dt
import pandas as pd

def concatenate_raw_text_files():
    
    """
    Created on Mon Mar  2 23:52:37 2015
    
    Takes all text files found in specific directory and concatenates; uses only the 
    header from the first file; user can specify input and output paths and filename
    
    @author: imchugh
    """    
    
    # Header info
    header_num_lines = 4    
    
    #Set file paths
    inpath = '/home/imchugh/Temp'
    target_files = ['Whroo_profile_IRGA_avg_1.dat','Whroo_profile_IRGA_avg_2.dat',
                    'Whroo_profile_IRGA_avg_offline1.dat','Whroo_profile_IRGA_avg_offline2.dat',
                    'Whroo_profile_IRGA_avg_offline3.dat']
    outpath = '/home/imchugh/Processing/Whroo/Profile data'
    outfile= 'Whroo_profile_IRGA_raw.dat'
    outpathfile=os.path.join(outpath,outfile)

    # Set working variables
    header_list=[]
    got_header=False
    
    # Open a file and write the file contents - skip header for write of all but first file
    with open(outpathfile, 'w') as outfile:    
        counter=0        
        for f in target_files:
            f=os.path.join(inpath,f)
            with open(f,'r') as infile:
                if counter == 0:
                    for line in infile:
                        outfile.write(line)
                    counter = counter + 1
                else:
                    for _ in xrange(header_num_lines):
                        next(infile)
                    for line in infile:
                        outfile.write(line)

def concatenate_TOA5_files():
    
    # File format
    skip_lines = [0,2,3]
    date_col = [0]
    na = ['NAN']
    interval_mins = 30

    # Set file paths
    # Input
    inpath = '/media/Data/Dropbox/Data/Logger data downloads 30minute/Whroo'
#    target_files = ['Whroo_profile_IRGA_avg.dat', 'Whroo_profile_IRGA_avg.dat.1.backup',
#                    'Whroo_profile_IRGA_avg.dat.backup','TOA5_Whroo_profile.IRGA_avg.dat']    
    target_files = ['Whroo_profile_Slow_avg.dat.backup','Whroo_profile_Slow_avg.dat.1.backup',
                    'Whroo_profile_Slow_avg.dat', 'TOA5_Whroo profile.Slow_avg.dat']
    check_subs = True
    # Output                    
    outpath = '/home/imchugh/Processing/Whroo/Profile data'
#    outfile = 'Whroo_profile_CO2.dat'
    outfile = 'Whroo_profile_met.dat'
    outpathfile = os.path.join(outpath,outfile)
    
    # Set output variables
#    target_variable_names = ['Cc_LI840_1m', 'Cc_LI840_2m', 'Cc_LI840_4m', 
#                             'Cc_LI840_8m', 'Cc_LI840_16m', 'Cc_LI840_32m']
    target_variable_names = ['Ta_HMP_1m_Avg', 'Ta_HMP_2m_Avg', 'Ta_HMP_4m_Avg', 
                             'Ta_HMP_8m_Avg', 'Ta_HMP_16m_Avg', 'Ta_HMP_32m_Avg', 'ps']
                             
    
    files_list = []
    df_list = []
        
    # Get subdirectories
    if check_subs:
        subs_list = [item[0] for item in os.walk(inpath)]
    
    # Find and open each file as pandas dataframe
    for f in target_files:
        
        # Locate files
        infile = os.path.join(inpath, f)
        if os.path.isfile(infile):
            files_list.append(infile)
        else:
            if not check_subs:
                print 'File ' + f + ' could not be found in directory ' + inpath + '!'
            else:
                file_found = False
                for subdir in subs_list:
                    infile = os.path.join(subdir, f)
                    if os.path.isfile(infile):
                        files_list.append(infile)
                        file_found = True
                if not file_found:
                    print 'File ' + f + ' could not be found in directory ' + inpath + ' or subdirectories!'

    for infile in files_list:

        df = pd.read_csv(infile, skiprows = skip_lines, parse_dates = date_col, 
                         index_col = date_col, na_values = na)
        df_list.append(df)
            
    for i in range(len(df_list)):

        # Check if desired variables are in all dataframes; if not, add dummy
        # and fill with NaNs
        df = df_list[i]
        check_vars_list = [name in df.columns for name in target_variable_names]
        if False in check_vars_list:
            false_indices = [j for j, x in enumerate(check_vars_list) if x == False]
            for index in false_indices:
                df[target_variable_names[index]]=np.nan
        
        # Subset dataframe to desired variables
        df_list[i] = df[target_variable_names]
    
    # Concatenate, sort, drop duplicates and pad missing timestamps
    full_df = pd.concat(df_list)
    full_df.sort_index(inplace = True)
    full_df.drop_duplicates(inplace = True)
    full_df = full_df.resample('2T', how = 'first')
    new_index = pd.date_range(full_df.index[0], full_df.index[-1], 
                          freq=str(interval_mins) + 'T')
    full_df = full_df.reindex(new_index)
    
    # Output    
    full_df.to_csv(outpathfile, index_label = 'TIMESTAMP')
    return full_df        
                    
def read_write_text_with_insert():
    
    # Header info
    header_num_lines = 4
    header_insert_dict = {1: '"RECORD"', 2: '"Unitless"', 3: '"Smp"'}
    
    inpath = 'C:\Temp\Data'
    target_file = 'Whroo_profile_IRGA_avg_offline3.dat'
    outpath = 'C:\Temp\Data'
    outfile = 'Whroo_profile_IRGA_avg_offline3_alt.dat'
    outpathfile = os.path.join(outpath, outfile)
    
    with open(outpathfile, 'w') as outfile:
        f = os.path.join(inpath, target_file)
        with open(f,'r') as infile:
            counter = 0
            for line in infile:
                if counter in header_insert_dict.keys():
                    insert_obj = header_insert_dict[counter]
                elif counter != 0:
                    insert_obj = str(counter)
                else:
                    insert_obj = None
                counter = counter + 1
                if not insert_obj == None:    
                    lst = line.split(',')
                    lst.insert(1, insert_obj)
                    line = ','.join(lst)
                    outfile.write(line)
                else:
                    outfile.write(line)
                    
def batch_file_rename():

    """
    Simple script to rename all files in directory
    - no arguments required, just edit the path, separator and prepend string
    """

    path = '/home/imchugh/Documents/Test'
    sep = ' '
    prepend_str = 'Dargo_'
    
    file_list = os.listdir(path)
    
    for fname in file_list:
        
        split_str = fname.split(sep)[1]
        
        new_fname = prepend_str + split_str
        
        os.rename(os.path.join(path, fname),
                  os.path.join(path, new_fname))