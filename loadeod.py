#!/usr/bin/python
"""
    loadeod - loads data from the  eoddata data provider

    future todo
        should I do a mass verify of all files to be the same at some point?

    about the file locations
    remote server : recent files are in main directory, older files are in history
    local server : all files are stored together

    platform specific issues to resolve
     filename - path/filename seperator needed to be switched from \ to /
     EODDATADIR - location of dir needed to be changed.
"""


import time
import string
import os
import datetime
import ftplib
import sys
import urllib
import MySQLdb
import pickle


#  paramater definitions for which series to load
activetypes=['INDEX','CBOT']    # not used at the moment.
# some series are duplicated among markets, we are appending market name to be specific
activeseries=[
'INDEX_DJI',
'INDEX_DJA',
'INDEX_DWC',
'INDEX_FVX',
'INDEX_GOX',
'INDEX_GLD',
'INDEX_GLI',
'INDEX_IQX',
'INDEX_IRX',
'INDEX_IUX',
'INDEX_IRX',
'INDEX_NNYL',
'INDEX_OIX',
'INDEX_OILBR',
'INDEX_OILSW',
'INDEX_RUT',
'INDEX_TNX',
'INDEX_TYX',
'INDEX_SPX',
'INDEX_VIX',
'INDEX_XOI',
'CBOT_ZT.C',
'CBOT_ZQ.C',
'CBOT_ZN.C',
'CBOT_ZG.C']
seriesvolume=['DJI']

# access data...activeseries.get('INDEX').count('WLD') > 0:

INTERACTIVE=0                            # plan to do a batch load of incremental updates
STR_TIME_FORMAT = '%m/%d/%Y %H:%M:%S'
site='ftp.eoddata.com'
user='<username>'
passwd='<password>'
rdir='History'   # the main dir has most recent data, History has the old stuff

if os.name=='posix':
    EODDATADIR='/data/eodmirror'
    sep='/'
else:
    EODDATADIR='C:\idx\eodmirror'
    EODSPECDIR='C:\idx\history'    # variable to point for special loads
    sep='\\'                        # this is a single backslash , don't know why two

DB_DUP_KEY=1062    # database error code

remotefilelist=[]
localfilelist=[]
remotefileloc={}

# use os.name to provide understanding of platform - 'nt' on windows

def yorn(ans,default):
    ans=string.upper(ans.strip())
    result=0
    if ans=='Y':
        result=1
    elif ans=='N':
        result=0
    elif ans=='':
        if default=='Y':
            result=1
    else:
        print 'Error: invalid answer(Y or N only)'
        print 'program terminated'
        sys.exit(0)
    return result

def recordfilename(filename):
    try:
        cursor.execute("""INSERT INTO eodfiles (filename) VALUES (%s)""",
                       (filename) )
    except MySQLdb.Error, e:
        errno=e.args[0]
        if errno==DB_DUP_KEY:
            print "Error, file %s in database, skipping" % (filename)
        else:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)
        
        
def insertecon(date, series, value):
    try:
        cursor.execute("""INSERT INTO econ (series, date, value)
                        VALUES (%s,%s,%s)""",
                         (series, date, value) )
    except MySQLdb.Error, e:
        errno=e.args[0]
        if errno==DB_DUP_KEY:
            cursor.execute("""UPDATE econ SET value=%s
                WHERE series=%s and date=%s""",
                             (value, series, date) )
            print 'updating record'
        else:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

def insertdata(item):
    # item is a list of the record from the eoddata file
    # guess at the format...series,date, ????hi, lo, open, close, volume ???
    # type is the catagory of eoddata - may be useful at times
    series=item[0]
    date=item[1]
    value=item[5] #!!!! need to verify!!!
    print 'inserting or updating: ', item
    # assert
    insertecon(date,series,value)
    #if seriesvolume.count(series)>0:  # want to store volume data too
    if 1: # lets load volumne all the time
        value=item[6] #!!!! need to recheck
        series=series+'.vol'
        #assert value.isnumeric() , 'value to insert is not numeric'
        insertecon(date,series,value)
        # ifs for other things like hi, lo, and open can be added here.

def loadfile(filename):
    if LOAD_SPEC_FILES:
        FILEDIR=EODSPECDIR
    else:
        FILEDIR=EODDATADIR
    fullfilename='%s%s%s' % (FILEDIR, sep, filename)
    fi=open(fullfilename,'r')
    eoddata=fi.readlines()
    fi.close
    market=filename.split('_')[0]  # 0 = 1st element, splitting on '_'
    print "the market is: ", market, " use this to match to the ACTIVETYPES"
    for line in eoddata:
        line=line.strip()
        item=line.split(',')  #!!! all items must be verified
        series=market + '_' + item[0]  # appending market name to series
        item[0]=series
        if activeseries.count(series) > 0:    # is this in the "watch list"
            insertdata(item)
    # delete file if no longer required....

def getremotefilelist():
    allremotefiles=[]
    remotefiles=ftpconn.nlst()  # do once for each directory - then create mstr list
    for filename in remotefiles:
        remotefileloc[filename]=''
    allremotefiles=remotefiles
# go after history files
    ftpconn.cwd(rdir)
    remotefiles=ftpconn.nlst()  # do once for each directory - then create mstr list
    for filename in remotefiles:
        remotefileloc[filename]='History'
    allremotefiles=allremotefiles+remotefiles
    #for filename in remotefiles:
    #    print filename
    return allremotefiles # !!! need to make sure we cat two lists!!!

def getlocalfilelist():
    localfiles=os.listdir(EODDATADIR)
    #for filename in localfiles:
    #    print 'localfile: ', filename
    return localfiles

def notloaded(filename):
    cursor.execute("""SELECT filename FROM eodfiles WHERE filename=%s""" ,
                   (filename))
    data=cursor.fetchall()
    #print "filename lookup response: ", data
    #print "rowcount ", cursor.rowcount
    notloadedvar=1
    if cursor.rowcount > 0:
        notloadedvar=0
    return notloadedvar

def getnewfiles(localfiles, remotefiles):
    thegetlist=remotefiles
# we used to compare to a list of files in the system to determine what we need to get.
# we now ceck the database witht he notloaded() procedure.
#    for filename in localfiles:
        #print 'cutting from local files: ', filename
#        found=0
#        while not found:
#            try:
                #print 'trying to remove: ', filename
#                thegetlist.remove(filename)   # need to manage the exception
                #print 'removed from get list: ', filename
#            except ValueError:
                #print 'not found in list: ', filename
#                found=1   # we expect some to exist and plan to ignore this error
    for filename in thegetlist:
            market=filename.split('_')[0]  # 0 = 1st element, splitting on '_'
            if (filename.count('.txt') > 0) and (activetypes.count(market) > 0):
                if notloaded(filename):
                    print 'getting file: ', filename, ' from: ', remotefileloc[filename]
                    getfile(filename)    # may need to look close at sub directorie
                    loadfile(filename)
                    print "**** should insert this file in db here"
                    recordfilename(filename)
            else:
                pass
                # print 'skipping: ', filename

def loadlocalfiles(localfiles):
    for filename in localfiles:
        print 'processing: ', filename
        loadfile(filename)

def getfile(filename):
    ftpdir='/' + remotefileloc[filename]
    ftpconn.cwd(ftpdir)

    # grab the last file
    localname=EODDATADIR + '/' + filename
    localfile=open(localname, 'w')
    callback=lambda line, file=localfile: file.write(line+'\n')
    ftpconn.retrlines('RETR ' + filename, callback)
    if not REPL_FILE_ONLY:
        loadfile(filename)
# end of getfile


print 'start loadeod 1.1 - ', time.strftime(STR_TIME_FORMAT, time.localtime())

#print sys.argv
#for arg in sys.argv:
#    if arg == '-r':
#        print 'Forced Reload'

REPL_FILE_ONLY=0
FORCE_RELOAD=0
LOAD_SPEC_FILES=0
LOAD_NORM_FILES=0

if INTERACTIVE:
    ans=yorn(raw_input('Load all local files(no sync of files):[y/N] '),'N')
    if ans==1:
        FORCE_RELOAD=1
        ans=yorn(raw_input('Load special files:[y/N] '),'N')
        if ans==1:
            LOAD_SPEC_FILES=1
        else:
            LOAD_NORM_FILES=1
    else:
        REPL_FILE_ONLY=1
else:
    REPL_FILE_ONLY=1


db = MySQLdb.connect(db="idxdb",user="root")
cursor = db.cursor()
if not FORCE_RELOAD:
    ftpconn=ftplib.FTP(site)
    ftpconn.login(user,passwd)
    remotefiles=getremotefilelist()         # files on remote server

localfiles=getlocalfilelist()           # files on local server
if LOAD_SPEC_FILES:                     # overwrite/ride the list and put in special files
    localfiles=os.listdir(EODSPECDIR)

if not FORCE_RELOAD:
    getnewfiles(localfiles,remotefiles)     # files are loaded in this module
else:
    loadlocalfiles(localfiles)

if not FORCE_RELOAD:
    ftpconn.quit()
db.close()
print 'eoj loadeod - ', time.strftime(STR_TIME_FORMAT, time.localtime())
sys.exit(0)

"""
next steps: test get files list, then do what full load or what...

notes: should we have a forced reload or some form of audit mode, in case data changed

"""
