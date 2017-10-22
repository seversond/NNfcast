#
# program linreg
#
# this is a test program to test the getseriesslpdata function 
# getseriesslpdata 

# added , date to the SELECT statements

import MySQLdb

# dtarray=value1.split('-')
# timelinefmjd=g2mjd(int(dtarray[0]),int(dtarray[1]),int(dtarray[2]))

def average(data):
    sum=0
    count=0
    for value in data:
        sum=sum+float(value)   # sum=sum+float(value[0])
        count=count+1
    return sum/count

def g2mjd(y, m, d):
# converts gregorian calendar dates to modified julian dates
  assert (y>1900) & (y<2021), 'year out of range in g2mjd: y=%d' % (y)
  assert (m>0) & (m<13), 'month out of range in g2mjd: m=%d' % (m)
  assert (d>0) & (d<32), 'day out of range in g2mjd: d=%d' % (d)
  if (m < 3):
    y=y-1
    m=m+12
  a=int(y/100)
  b=2-a+int(a/4)
  jd=int(365.25*(y+4716)) + int(30.6001*(m+1)) + d + b - 1524.5
  mjd=int(jd - 2400000.5)
  #print 'mjd', mjd
  assert (mjd>0), 'mjd out of range in g2mjd: %d' % (mjd)
  return mjd



def mjd2g(mjd):
    assert (mjd>44238 & mjd<58850), 'mjd is out of range: %d' % (mjd)
    # assert (mjd<#####), 'mjd later then 12312020'
    jd=mjd+2400000.5
    z=int(jd+.5)
    f=jd+0.5-int(jd+0.5)
    if z<2299161: a=z
    else:
        alpha=int((z-1867216.25)/36524.25)
        a=z+1+alpha-int(alpha/4)

    b=a+1524
    c=int((b-122.1)/365.25)
    d=int(365.25*c)
    e=int((b-d)/30.6001)

    dy=b-d-int(30.6001*e) # if we want fraction of a day "+f"
    if e<14:
        mn=e-1
    else:
        mn=e-13
    if mn>2:
        yr=c-4716
    else:
        yr=c-4715

    assert (e<16), 'e out of range: %d' (e)
    assert (dy>0 & dy<32), 'day out of range: %d' % (dy)
    assert (mn>0 & mn<13), 'month out of range: %d' % (mn)
    assert (yr>1979 & yr<2020), 'year out of range or has not been tested: %d %d' % (yr,mjd)

    return (yr, mn, dy)


def getseriesdata(x,rdate,datasrc):
    # recheck how this was done before
    series=x[0]
    elements=int(x[1])
    lag=0         # lag will be determined by doing a dictionary lookup lag=lags{series}?
    if nolags: lag=0        # used for production forecasting when we want the best data avail.
    rdateg=mjd2g(rdate-lag)
    qdate=str(rdateg[0])+'-'+str(rdateg[1])+'-'+str(rdateg[2])
    if datasrc=='econsmooth':
        cursor.execute("""SELECT value, date  FROM econsmooth WHERE series=%s AND date < %s order by date desc LIMIT %s""" ,
              (series, qdate, elements))
    else:
        cursor.execute("""SELECT value, date FROM econ WHERE series=%s AND date < %s order by date desc LIMIT %s""" ,
              (series, qdate, elements))
    data=cursor.fetchall()
    # seems to come out of here as a 2xn array, but the [0] column is used and the [1] is empty
    assert cursor.rowcount>0, 'no data returned - rdate=%d series=%s' % (rdate, series)
    return data

def getseriesslpdata(x,rdate,datasrc):
    data=getseriesdata(x,rdate,datasrc)
    print data
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
    print x
    print y
    print "start to do averages"
    meanx=average(x)
    meany=average(y)
    print meanx
    print meany
    n=0
    d=0
    for pair in mjddata:   # !!! determine how to set data
        n=n+(pair[0]-meanx)*(pair[1]-meany)
        d=d+(pair[0]-meanx)**2
        print pair[0], pair[1]
    print "n=",n
    print "d=",d
    b=n/d
    # do the slope calculations here
    return b  # will need to return slope

# start

print "start of linreg"
db = MySQLdb.connect(db="idxdb",user="root")
cursor = db.cursor()
nolags=1
# 49987 = 27 sept 1995
x=["INDEX_SPX",10]
retdata=getseriesslpdata(x,49987,"econ")
print retdata

x=["INDEX_SPX",5]
retdata=getseriesslpdata(x,49987,"econ")
print retdata

x=["INDEX_SPX",2]
retdata=getseriesslpdata(x,49987,"econ")
print retdata

x=["INDEX_SPX",50]
retdata=getseriesslpdata(x,49987,"econ")
print retdata

x=["INDEX_SPX",10]
retdata=getseriesslpdata(x,49987,"econ")
print retdata

x=["INDEX_SPX",20]
retdata=getseriesslpdata(x,49800,"econ")
print retdata

x=["INDEX_SPX",10]
retdata=getseriesslpdata(x,51000,"econ")
print retdata

x=["INDEX_SPX",10]
retdata=getseriesslpdata(x,51100,"econ")
print retdata

db.close()
print "linreg end"
