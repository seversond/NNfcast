"""
   normalize


"""

import time
import string
import os
import datetime
import sys
import MySQLdb

VERSION = '1.0'
STR_TIME_FORMAT = '%m/%d/%Y %H:%M:%S'

# run parameters
PARM_CALCFACTORS=1         #
PARM_LOADNORMAL=0          # normalizes all data
PARM_LOADNORMALINC=0       # only loads data that has not been loaded already, imcremental load


def calcfactors(series):
    # print 'processing series: ', series
    cursor.execute("""SELECT date, value FROM econsmooth
              WHERE series=%s order by date""" ,
              (series))
    data_to_normalize=cursor.fetchall()
    rowcount=cursor.rowcount
    cursor.execute("""SELECT min(value), max(value) FROM econsmooth
              WHERE series=%s""" ,
              (series))
    min_max_results=cursor.fetchall()
    min_of_series=min_max_results[0][0]
    max_of_series=min_max_results[0][1]
    cutoff=min_of_series
    diff=max_of_series-min_of_series
    if diff<>0:
        factor=1/(max_of_series-min_of_series)
    else:
        factor=0
        print 'WARNING: factor set to zero to avoid divide by zero error. check series: ', series

    cursor.execute("""UPDATE econdat SET normfactor=%s, normcutoff=%s
           WHERE series=%s""", (factor, cutoff, series))
    print 'Series=', series, ' cutoff=', cutoff, ' factor=', factor, ' elements=', rowcount


print 'start normalize ', VERSION, ' - ', time.strftime(STR_TIME_FORMAT, time.localtime())

#print sys.argv
#for arg in sys.argv:
#    if arg == '-r':
#        print 'Forced Reload'

db = MySQLdb.connect(db="idxdb",user="root")
# if we are doing one series we can skip the gather section. maybe just set data to include
#        one series
cursor = db.cursor()
cursor.execute("""SELECT distinct series FROM econ""")
list_of_series=cursor.fetchall()


if PARM_CALCFACTORS:
    for item in list_of_series:
        series=item[0]
        calcfactors(series)
"""     we may not use this...
if PARM_LOADNORMAL:
    for item in list_of_series:
        series=item[0]
        if not PARM_LOADNORMALINC:
            print 'deleting normalize data for series: ', series'
            cursor.execute("delete from econnormal where series=%s",
                        (series))
        storenormalize(series)
    # storesmooth used to be at the end of processseries - make sense to seperate now
    #storesmooth(alpha, series, orig, orig_date)
    # the old data orig orig_date is probably not required
"""

db.close()
print 'eoj normalize - ', time.strftime(STR_TIME_FORMAT, time.localtime())
sys.exit(0)
