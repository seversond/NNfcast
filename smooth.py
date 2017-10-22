
"""
   NOTE: you must do CALCALPHA when adding a new series.

   smooth
       this program is designed to find the optimal exponential smoothed
       curve of the original financial data.

   restictions...
       - only does a smooth of a whole series
       - only does the best fitted exponetial smooth curve(only one)

   tuning opportunities
       - different smoothing strategies
       - smoothing strategy that uses min error approach rather than fixed 20 iterns

"""

import time
import string
import os
import datetime
import sys
import MySQLdb

VERSION = '2.0'
STR_TIME_FORMAT = '%m/%d/%Y %H:%M:%S'

# run parameters
PARM_CALCALPHA=0 # last done 3/25/07, calculates alpha at run time with all available data, stores in econdat
PARM_LOADSMOOTH=1          # loads all smooth data from available raw data
PARM_LOADSMOOTHINC=0# not tested!# only loads data that has not been loaded already, imcremental load

def geterror(alpha, orig):
    total_error=0
    rows=len(orig)
    count_err=0
    for element in orig:    # 0? or 1?      (count=1; count<=rows; count++):
       value=element[1]
       if (count_err == 0):
           s=value
       else:
           s=next_s
       count_err=count_err+1
       next_s=alpha*value + (1-alpha)*s
       #print value, s, count_err, rows
       error=abs(value-s)
       total_error=total_error+error
    avg_error=total_error/rows
    return avg_error
# end of geterror

def storesmooth(series):
    cursor.execute("""SELECT alpha FROM econdat
          WHERE series=%s""" ,
          (series))
    element=cursor.fetchall()
    if cursor.rowcount==0:
        print 'ERROR: series not in econdat: ', series, ' need to do a PARM_CALC alpha'
        sys.exit(2)
    alpha=element[0][0]
    cursor.execute("""SELECT date, value FROM econ
          WHERE series=%s order by date""" ,
          (series))
    seriesdata=cursor.fetchall()  # elements are (date, value)
    # the above assumes a full resmooth.  inc. would start with changes above
    firsttime=1
    for element in seriesdata:
        value=element[1]
        if firsttime:
            s=value
        else:
            s=next_s
        firsttime=0
        next_s=alpha*value + (1-alpha)*s
        # NOTE: we are only storing one smooth set per series
        # make changes here to add more than one smooth set per series
        #print series, orig_date[item], s, item
        cursor.execute("""INSERT INTO econsmooth (series, date, value)
                        VALUES (%s,%s,%s)""", (series, element[0], s))
    # need to store the scaling factors for later recall


# end of storesmooth

def calcalpha(series):
    # do all series of a limited set?
    p1=0.0
    p2=0.0
    p1_error=0.0
    p2_error=0.0
    print 'processing series: ', series
    cursor.execute("""SELECT date, value FROM econ
              WHERE series=%s order by date""" ,
              (series))
    seriesdata=cursor.fetchall()
    rowcount=cursor.rowcount
    next=''
    #rows=len(seriesdata)
    #for element in seriesdata:
    #    orig_date.append(element[0])
    #    orig.append(element[1])

    first_time=1
    count=0        #
    lower=0
    upper=1
    stepsize=0.3   # probably should be between 0 and .5

    lbounds=0.0
    ubounds=1.0

    for count in range(20):                        # usually the do this until we reach a certain min
        step= (ubounds-lbounds) * stepsize;
        if (next == 'R'):
            point=lbounds+step
            p1=lbounds
        else:
            point=ubounds-step
            p1=ubounds
        p2=point
        #p2_error=geterror(p2,orig)
        #p1_error=geterror(p1,orig)
        p2_error=geterror(p2,seriesdata)
        p1_error=geterror(p1,seriesdata)

        slope= (p2_error-p1_error) / (p2-p1)
        if (slope < 0):
            lbounds=point
            next='R'
        else:
            ubounds=point
            next='L'
        #print ubounds, lbounds, p1_error, p2_error
        # end of for

    alpha= (ubounds+lbounds) / 2
    print 'Series=', series, ' Alpha=', alpha, ' elements=', rowcount
    cursor.execute("""SELECT series FROM econdat WHERE series=%s""", (series))
    if cursor.rowcount==1:
        cursor.execute("""UPDATE econdat SET alpha=%s
               WHERE series=%s""", (alpha, series))
    else:
        cursor.execute("""INSERT INTO econdat(series,alpha) VALUES (%s, %s)""",
                       (series, alpha))

print 'start smooth ', VERSION, ' - ', time.strftime(STR_TIME_FORMAT, time.localtime())

#print sys.argv
#for arg in sys.argv:
#    if arg == '-r':
#        print 'Forced Reload'

db = MySQLdb.connect(db="idxdb",user="root")
# if we are doing one series we can skip the gather section. maybe just set data to include
#        one series
cursor = db.cursor()
cursor.execute("""SELECT distinct series FROM econ""")
series_to_process=cursor.fetchall()


if PARM_CALCALPHA:
    #x=1
    for item in series_to_process:
        series=item[0]
        calcalpha(series)
        #x=x+1
        #if x==3:            # stop early for debugging
        #    sys.exit(0)
if PARM_LOADSMOOTH:
    for item in series_to_process:
        series=item[0]
        print 'loading smooth data for series: ', series
        if not PARM_LOADSMOOTHINC:
            print 'deleting all smooth data for series: ', series
            cursor.execute("""delete from econsmooth where series=%s""",
                           (series))
        storesmooth(series)
    # storesmooth used to be at the end of processseries - make sense to seperate now
    #storesmooth(alpha, series, orig, orig_date)
    # the old data orig orig_date is probably not required

db.close()
print 'eoj smooth - ', time.strftime(STR_TIME_FORMAT, time.localtime())
sys.exit(0)
