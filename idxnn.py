#!/usr/bin/python

"""

Name:        idxnn
Purpose:     prepare records and train a neural network according
                to specification in <TBD>
Execution:   intended to be run as a batch program
Inputs:      parameter file(text file) -- hardcoded inline at this time
             idxdb database
Outputs:     neural network(file???)
             audit file(text file)
Libraries:   MySQLdb, NumPy

todo:
             need to build in a forecasting mode that uses this record
                 construction strategy
             does date need to be ascending or decending on the get
             need an order by in the select
             1 - check for error codes on sql statements???
             do better validation checks on series

Assumptions:
             need to define lags in reporting of data for training
             testing will need to respond to lags
             realtime forecasting will completely ignore lags
             rdates need to be a valid "market day"
             forecasted dates must be valid "market days"
             valid market days are determined by sp500 closing numbers in db
             assume that average lag time will be sufficient- ouch!
             should forecase schema be a class?

"""
progver="1.4"

import time
import datetime
import MySQLdb
import sys
import math
#from numpy import average
from idxutils import average, doy, g2mjd, mjd2g, rand
from idxbpnn import NN

STR_TIME_FORMAT = '%m/%d/%Y %H:%M:%S'

mktseries='INDEX_SPX'    # series used as the indication if it is a
fcast_series=mktseries

# --- define default parameters
parm_ident='undefined'
parm_anlsaw=0
parm_anlsin=0
parm_anlcos=0
parm_hinodes=0
parm_hlayers=1
parm_netm=0.5
parm_netn=0.1
parm_portion_testdata=0.2    # set portion of test data to 20% by default
parm_seriesprm=[]
parm_seriesmaprm=[]
parm_seriesslpprm=[]

valid_firstdate=47892 # 1990-1-1
valid_lastdate=55927 # 2012-1-1

# initialize command line params
forecastmode="yes"    # default to forecast mode
spec_model=''
spec_firstdate=''
spec_lastdate=''
# default timeline for models
timelinefmjd=g2mjd(1990,1,1)
timelinelmjd=g2mjd(2020,1,1)
timeline_sfactor=1/float(timelinelmjd-timelinefmjd)
# default the first and last date if command line or models do not specify
firstdatemjd=g2mjd(2007,1,5)
lastdatemjd=g2mjd(2007,5,5)
# netfile="fakefilename.txt"   --- not used

normaldat={}

def getparms(ident):
    global parm_ident
    cursor.execute("""select parm, value1, value2 from models where
                id=(select id from models where parm='ident'and value1=%s)""" , (ident))
    parmdata=cursor.fetchall()
    # print 'getparms: ', parmdata
    assert cursor.rowcount>0, 'no series found - ident=%s' % (ident)
    for parmrow in parmdata:
        print "parmrow:", parmrow
        parm=parmrow[0]
        value1=parmrow[1].strip()
        if len(parmrow)>2:
            value2=parmrow[2]
        if parm=='ident':
            # print 'ident pulled from db: ', value1
            parm_ident=value1
        elif parm=='series':
            parm_seriesprm.append([value1, value2])
            print value1, value2
            assert value2<>'', "getparms: parameter series is missing value2"
        elif parm=='seriesma':
            parm_seriesmaprm.append([value1, value2])
            assert value2<>'', "getparms: parameter seriesma is missing value2"
        elif parm=='seriesslp':
            parm_seriesslpprm.append([value1, value2])
            assert value2<>'', "getparms: parameter seriesslp is missing value2"
        elif parm=='anlsaw':   parm_anlsaw=value1
        elif parm=='anlsin':   parm_anlsin=value1
        elif parm=='anlcos':   parm_anlcos=value1
        elif parm=='timeline':
            dtarray=value1.split('-')
            timelinefmjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
            dtarray=value2.split('-')
            timelinelmjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
            timeline_sfactor=1/float(timelinelmjd-timelinefmjd)
        elif parm=='hinodes':  parm_hinodes=value1
        elif parm=='hlayers':  parm_hlayers=value1
        elif parm=='netm':     parm_netm=value1
        elif parm=='netn':     parm_netn=value1
        elif parm=='dates':
            dtarray=value1.split('-')
            firstdatemjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
            dtarray=value2.split('-')
            lastdatemjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
        elif parm=='fseries':
            parm_fseries.append(value1)
            parm_fseries.append(value2)
#'params are: ident, seriesma, series, anlsaw/sin/cos, timeline(2), hidnodes,'
# --- for some reason I thought seriesma had 2 parameters, but I am switching back to 1
#'hlayers, netm, netn, dates(2)'
# end

def getlags():
    pass
    # !!! determine lags by scanning the series listed in the parms_

def getnormaldat():
    global normaldat
    def appendnormaldat(series):
        if normaldat.has_key(series):
            pass
            print 'normaldat already has series: ', series
        else:
            print 'looking up normaldat for series to load: ', series
            # db lookup here
            cursor.execute("""SELECT normfactor, normcutoff FROM econdat WHERE series=%s""" ,
              (series))
            data=cursor.fetchall()
            assert cursor.rowcount>0, 'getnormaldat: no rows in econdat series=%s' % (series)
            normfactor=data[0][0]
            normcutoff=data[0][1]
            normaldat[series]=[normfactor,normcutoff]
    for seriesset in parm_seriesprm:    # loop through all the parms for a series in use
        series=seriesset[0]
        appendnormaldat(series)
    for seriesset in parm_seriesmaprm:  # loop through all the ma parms for series in use
        series=seriesset[0]
        appendnormaldat(series)
    for seriesset in parm_seriesslpprm: # loop .... in use
        series=seriesset[0]
        appendnormaldat(series)
    #for series in normaldat.keys():
        #print normaldat[series]


def getseriesdata(x,rdate,datasrc):
    # recheck how this was done before
    series=x[0]
    elements=int(x[1])
    lag=0         # lag will be determined by doing a dictionary lookup lag=lags{series}?
    if nolags: lag=0        # used for production forecasting when we want the best data avail.
    rdateg=mjd2g(rdate-lag)
    qdate=str(rdateg[0])+'-'+str(rdateg[1])+'-'+str(rdateg[2])
    if datasrc=='econsmooth':
        cursor.execute("""SELECT value FROM econsmooth WHERE series=%s AND date < %s order by date desc LIMIT %s""" ,
              (series, qdate, elements))
    else:
        cursor.execute("""SELECT value FROM econ WHERE series=%s AND date < %s order by date desc LIMIT %s""" ,
              (series, qdate, elements))
    data=cursor.fetchall()
    # seems to come out of here as a 2xn array, but the [0] column is used and the [1] is empty
    assert cursor.rowcount>0, 'no data returned - rdate=%d series=%s' % (rdate, series)
    return data
# end getseriesdata

def getseriesmadata(x,rdate,datasrc):
    data=getseriesdata(x,rdate,datasrc)
    return average(data)
# end getseriesmadata

def getseriesslpdata(x,rdate,datasrc):
    data=getseriesdata(x,rdate,datasrc)
    # print data
    x=[]
    y=[]
    mjddata=[]                      # same as data but the dates are converted to mjd
    mjddata=[]
    for item in data:
        yr=item[1].strftime('%Y')
        mn=item[1].strftime('%m')
        dy=item[1].strftime('%d')
        #print yr, mn, dy
        mjdate=g2mjd(int(yr),int(mn),int(dy))
        print item[0], mjdate
        x.append(mjdate)
        y.append(item[0])
        newitem=[mjdate,item[0]]
        mjddata.append(newitem)
        # ok now these two values can be calculated for the linreg...
    # print x
    # print y
    # print "start to do averages"
    meanx=average(x)
    meany=average(y)
    # print meanx
    # print meany
    n=0
    d=0
    for pair in mjddata:   # !!! determine how to set data
        n=n+(pair[0]-meanx)*(pair[1]-meany)
        d=d+(pair[0]-meanx)**2
        print pair[0], pair[1]
    # print "n=",n
    # print "d=",d
    b=n/d
    # do the slope calculations here
    return b  # will need to return slope

def getprediction(series,rdate,elements):
    rdateg=mjd2g(rdate)
    qdate=str(rdateg[0])+'-'+str(rdateg[1])+'-'+str(rdateg[2])
    elements=1 #!??!?!?!?  I must have copied the select statement in without considering approach, rethink it!
    # do we get this from econ or econsmooth??
    # todo: test with element =1 adjust query to get n elements and sort to take the
    #      top one on the list
    cursor.execute("""SELECT value FROM econ
              WHERE series=%s AND date > %s order by date desc LIMIT %s""" ,
              (series, qdate, elements))
    data=cursor.fetchall()
    assert cursor.rowcount>0, 'no data returned - rdate=%d (%s)' % (rdate,qdate)
    return data

def trainingdata(value,valuesrc,type="input"):
    global titems
    global tinputs
    global toutput
    #
    #  normalize data here
    #            * for valuesrc indicates that the data is already normalized and we can skip this process
    if valuesrc <> '*':
        series=valuesrc
        normfactor=normaldat[series][0]
        normcutoff=normaldat[series][1]
        nvalue=(value-normcutoff)*normfactor  # this is the normalize step
    else:
        nvalue=value   #already normalized
    assert ((nvalue>=-0.5) & (nvalue<=1.5)), 'training value not normalized?: %f' % (nvalue)
    if type=="input":
        tinputs.append(nvalue)
    else:
        toutput.append(nvalue)
    titems=+1


def close_training_rec():
    global titems, trecs, old_items
    global tdata
    global trndata
    global tstdata
    global tinputs
    global toutput
    trecord=[]
    trecord.append(tinputs)
    trecord.append(toutput)
    # do a probability funciton here to determine if they are appended to tdata or tstdata
    #    the % of records in tstdata is parm_portion_testdata
    #    need to set random function
    if rand(1,100) > 10:
        trndata.append(trecord)
    else:
        tstdata.append(trecord)
    # trndata.append(trecord)####### old number
    # assert  if old_titems=titems) print "ERROR: difference in number of items
        # in traning rec titems, oldtitems, trecs
    old_titems=titems
    titems=0
    trecs=+1
# end close_training_record


def validmktday(series, rdate):
    rdateg=mjd2g(rdate)
    qdate=str(rdateg[0])+'-'+str(rdateg[1])+'-'+str(rdateg[2])
    #print "validmktday: ", series, rdate, qdate
    cursor.execute("""SELECT value FROM econ
              WHERE series=%s AND date=%s""" ,
              (series, qdate))
    data=cursor.fetchall()
    assert cursor.rowcount==1 or cursor.rowcount==0, 'validmktday: invalid row count %s %s' % (series, qdate)
    # print "validmktday: ", cursor.rowcount
    if cursor.rowcount==1:
        validday=1
    else:
        validday=0
    print validday
    return validday
# end validmktday


# function makerecs
def makerecs(forecastmode):
    global tinputs
    global toutput
    global firstdatemjd
    global lastdatemjd
    #
    # value - refers to a data element that is fully prepared to go into trng file
    if (forecastmode=="yes"):
        lastdatemjd=firstdatemjd+1
        print "forecast mode is YES so first and lastmjd is the same"
    print firstdatemjd
    print lastdatemjd
    for rdate in range(firstdatemjd, lastdatemjd):
        tinputs=[]
        toutput=[]
        # todo: check for valid market day
        # todo op: check for valid market day in forecast
        rdateg = mjd2g(rdate)
        print 'rdate is: %d (%4d-%02d-%02d)' % (rdate, rdateg[0], rdateg[1], rdateg[2])
        print 'mktseries: ', mktseries
        if validmktday(mktseries,rdate):
            # for series in
            for x in parm_seriesprm:                # x is (series, elements, lag)
                valuesrc=x[0]
                datasrc='econsmooth'
                data=getseriesdata(x,rdate,datasrc)
                for value in data:
                    trainingdata(value[0],valuesrc)   # use [0] to make sure it is a simple value
            # for seriesma in
            for x in parm_seriesmaprm:
                valuesrc=x[0]
                datasrc='econ'
                value=getseriesmadata(x,rdate,datasrc)
                trainingdata(value,valuesrc)       # the value is simple now after going through average
            # do remaining items
            if ( timelinefmjd>0 ):
                valuesrc='*'
                value=(rdate - timelinefmjd)*timeline_sfactor;
                #print value
                trainingdata(value,valuesrc)
            if ( parm_anlsaw ):
                valuesrc='*'
                value=doy(rdateg)/366
                trainingdata(value,valuesrc)

            if ( parm_anlsin ):
                valuesrc='*'
                value=sin(doy(rdateg)/366*pi)/2+0.5
                trainingdata(value,valuesrc)

            if ( parm_anlcos ):
                valuesrc='*'
                value=cos(doy(rdateg)/366*pi)/2+0.5
                trainingdata(value,valuesrc)

    # forecast mode:  would skip this section, write a zero or leave the record one element short?
            # get prediction
            fcseries='INDEX_SPX'
            fcelements=1
            value=getprediction(fcseries, rdate, fcelements)
            valuesrc=fcseries
            trainingdata(value[0][0],valuesrc,"forecast")            # use [0] to make sure it is a scalor value
            # print 'forecast value: ', value[0][0], rdate

            close_training_rec()
        else:
            pass
            print "non-market day: ", mjd2g(rdate)
# end function makerecs

def calc_fdate(ident, rdate):
    # get forecast stuff from model
    #public static DateTime AddWeekdays( DateTime start, int days )
    # note that this procedure is forward looking but does NOT take into consideration
    # holidays, such as, labord, memd, 4th, christmas, thgvng.  This will probably provide a good approximation
    # but corrections can be applied easily after the fdate passes
    
    #remaindr = days % 5
    #weekendDays = ( days / 5 ) * 2

    #end = start.AddDays( remaindr )

    #if ( end.DayOfWeek == DayOfWeek.Saturday ):        # was working here last...?
      # add two days for landing on saturday
    #  end = end.AddDays( 2 )
    #elif ( end.DayOfWeek < start.DayOfWeek ):
      # add two days for rounding the weekend
    #  end = end.AddDays( 2 )
    # add the remaining days
    return 0  # end.AddDays( days + weekendDays - remaindr )
# end calc_fdate

def recordforecast(ident, timestamp, progver, forecast, elapsed, variables, rdate, fseries):
    # write to database model, time/date, forecast, time, variables
    # get unnormalized values
    # fvalue=
    # rvalue=
    # nvalue= (normalized version of the reference date)
    # nepoch= current epoch?
    #
    fdate=calc_fdate(ident, rdate)  # don't know what ident is for?
    try:
        cursor.execute("""INSERT INTO forecastruns (ident, timestamp, progver, forecast, elapsed, variables)
                    VALUES (%s,%s,%s,%s,%s,%s)""",
                     (ident, timestamp, progver, forecast, elapsed, variables) )
                    # rdate, fdate  ... can be added later.  Not in the db at the moment.
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit (1)
# end recordforecast

def recordtraining(ident, timestamp, progver, error, elapsed, variables, records):
    # write to database model, time/date, error, time, variables, records
    try:
        cursor.execute("""INSERT INTO trainruns (ident, timestamp, progver, error, elapsed, variables, records)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                     (ident, timestamp, progver, error, elapsed, variables, records) )
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit (1)
# end recordtraining


"""
================================================================================
 main starts here
================================================================================
"""

print "--- starting odnn v", progver, " ",time.strftime(STR_TIME_FORMAT, time.localtime())

forecastmode="yes"  # remember to review first and last dates

#print "parameters: ", len(sys.argv)
print "argv: ", sys.argv
if (len(sys.argv)>1):
    forecastmode=sys.argv[1]
if (len(sys.argv)>2):
    spec_model=sys.argv[2]
    ident=spec_model
if (len(sys.argv)>3):            # not used yet
    spec_firstdate=sys.argv[3]
    dtarray=spec_firstdate.split('-')
    firstdate_mjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
    assert (firstdate_mjd > valid_firstdate), \
        'invalid date, too early val: %d ref: %d' % (firstdate_mjd, valid_firstdate)    
if (len(sys.argv)>4):            # note used yet
    spec_lastdate=sys.argv[4]
    dtarray=spec_lastdate.split('-')
    lastdate_mjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
    assert (lastdate_mjd < valid_lastdate), \
        'invalid date, too late val: %d ref: %d'  % (lastdate_mjd, valid_lastdate)
    # should be compared to now.  now is relative
if ( forecastmode=="yes" ):            # turn on forecast mode
    nolags=1
    netfilemode='r'
    print 'doing forecast'
else:
    nolags=0
    netfilemode='w'
    print 'doing a training run'

time1 = time.time()

db = MySQLdb.connect(db="idxdb",user="root")
cursor = db.cursor()

tdata=[]         # training data variable
trndata=[]        # training data patterns/sets
tstdata=[]        # testing data patterns/sets
tinputs=[]       # training record
titems_p=0
#ident='allin1'
getparms(ident)
# override the model with dates if they were specified at the command line
if spec_firstdate<>'':
    dtarray=spec_firstdate.split('-')
    firstdatemjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
if spec_lastdate<>'':        # not possible to just override lastdate (4th parm on line
    dtarray=spec_lastdate.split('-')
    lastdatemjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))
getlags()
getnormaldat()
# print normaldat.keys()

# ident, series, seriesma, anlsaw, anlsin, anlcos, timeline, hidnodes, dates, hlayers,
#     netm, netn
# odnn would pass in ident and get all these parameters
# parms
# pickle this in the future?
print "parm_seriesprm: ", parm_seriesprm
for name, slots in parm_seriesprm:
    print "name and slots: ", name, slots
    titems_p = titems_p + int(slots)

titems_p= titems_p + len(parm_seriesmaprm)

net_hidden_node_factor=0.8    # fractional size of hidden layer one as compared to input layer

titems_p= titems_p + (parm_anlsaw + parm_anlsin + parm_anlcos)
if ( timelinefmjd ): titems_p= titems_p + 1

# print ident
print 'ident: ', parm_ident
print 'annual cycles(s,s,c): ', parm_anlsaw, parm_anlsin, parm_anlcos
print 'series slots...'
for row in parm_seriesprm:
    print row
print 'seriesma - moving averages...'
for row in parm_seriesmaprm:
    print row
print 'seriesslp - series slope...'
for row in parm_seriesslpprm:
    print row
fdateg=mjd2g(firstdatemjd)
fdate=str(fdateg[0])+'-'+str(fdateg[1])+'-'+str(fdateg[2])
ldateg=mjd2g(lastdatemjd)
ldate=str(ldateg[0])+'-'+str(ldateg[1])+'-'+str(ldateg[2])
print 'rdate range: %s (%d) - %s (%d)' % (fdate,firstdatemjd,ldate,lastdatemjd)
print 'elements: ', titems_p+1

print '--- assembling records now ', time.strftime(STR_TIME_FORMAT, time.localtime())
sys.stdout.flush()
# now assemble the records
makerecs(forecastmode)
# assert (trec_p= trec) print "ERROR: param specification of elements
#        different than actual number of records present:" trecs, trecs_p

#print 'elements in second rec: ', len(trndata[1][0])
# do as ASSERT --- print 'elements in last rec: ', len(trndata[len(trndata)-1][0])
# debug statement? print trndata[0]

time2 = time.time()
sys.stdout.flush()

netinputs=titems_p
nethidden=int(float(titems_p*net_hidden_node_factor))
netoutputs=1
netfile=parm_ident      # for test only, need to determine file naming strategy
if ( forecastmode=="yes" ):
    print '--- starting forecast ', time.strftime(STR_TIME_FORMAT, time.localtime())
# forecast mode read network, execute network, store output
    #load net
    print netinputs, nethidden, netoutputs
    net=NN(netinputs, nethidden, netoutputs)        # create the network
    net.restore(netfile)
    print trndata
    forecast=net.runnet(trndata)
    # run net
else:
    print 'elements in rec: ', len(trndata[0][0])
    print 'FIX?.................training(trn) records: ', len(trndata)
    print 'FIX?.................test(tst) records: ', len(tstdata)
    print '--- start training ', time.strftime(STR_TIME_FORMAT, time.localtime())
    print netinputs, nethidden, netoutputs
    net=NN(netinputs, nethidden, netoutputs)        # create the network
    # training mode train network, save network, store performance of net
    netfit=net.trainnet(trndata,tstdata,parm_netn,parm_netm)  # train the network
    net.save(netfile)                              # save the network

time3 = time.time()

# calc time

recpreptime=time2-time1
traintime=time3-time2
runtime=time3-time1

# record the over of the results...
eojtime=datetime.datetime.now()
if ( forecastmode=="yes" ):            # turn on forecast mode
    print 'forecast(normalized):', forecast[0]
    normfactor=normaldat[fcast_series][0]
    normcutoff=normaldat[fcast_series][1]
    fvalue=forecast[0]/normfactor + normcutoff # the rvrs...nvalue=(value-normcutoff)*normfactor
    print 'forecast series: ', fcast_series, ' value: ', fvalue, ' ...<date, fdate>' 
    rdate=0   # need to get real date out of the getforecast ...
    recordforecast(ident, eojtime, progver, forecast[0], runtime, len(trndata[0][0]), rdate, fcast_series)
else:
    #
    err_filler=0 # should we use netfit?
    recordtraining(ident, eojtime, progver, err_filler, runtime, len(trndata[0][0]), len(trndata))

#
#  hard work is done close down program
#

# should we store? ident, netfit, sequencenum, date, runtime in database?
# status output
print '--- train/run done', time.strftime(STR_TIME_FORMAT, time.localtime())
print 'not correct...training records: ', len(trndata)  # Not correct!
print 'testing records: ', len(tstdata)
print 'not correct...elements: ', len(trndata[0])        # not correct!
print trndata[0]
print 'total time to complete: %.3f mins' % (runtime/60)
print 'rec prep time to complete: %.3f mins' % (recpreptime/60)
if ( forecastmode<>"yes" ): 
    print 'train time to complete: %.3f mins' % (traintime/60)
print '--- EOJ odnn', time.strftime(STR_TIME_FORMAT, time.localtime())

# close
db.close()
# tfile.close() not used at this time
# afile.close()  # openned in odbpnn.py
# nfile.close()

sys.exit(0)     # assume successfully if we get to this statement.
