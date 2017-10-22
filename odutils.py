"""

Name:    idxutils
Purpose: various utilities useful to a wide variety of idx programs

"""
import math
import random

def rand(a, b):
    return (b-a)*random.random() + a

# find the average of a string of numbers in an array
def average(data):
    sum=0
    count=0
    for value in data:
        sum=sum+float(value[0])
        count=count+1
    return sum/count

# function doy - provides the day of year in numerical form
def doy(gdate):
    y=gdate[0]
    m=gdate[1]
    d=gdate[2]
    k=2
    n=math.floor(275*m/9) - k*math.floor((m+9)/12) + d -30
    assert ((n>0) & (n<367)), 'day of year out of range at end of doy: %d' % (n)
    return n
# end of doy

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
