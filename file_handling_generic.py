# -*- coding: utf-8 -*-


import pdb
import os
from datetime import datetime as dt

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
    outpath = '/home/imchugh/Processing/Whroo/Profile data'
    outfile= 'Whroo_profile_IRGA_raw.dat'
    outpathfile=os.path.join(outpath,outfile)
    dirList = os.listdir(inpath)
    dirList.sort()

    # Set working variables
    header_list=[]
    got_header=False
    
    # Open a file and write the file contents - skip header for write of all but first file
    with open(outpathfile, 'w') as outfile:    
        counter=0        
        for f in dirList:
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
