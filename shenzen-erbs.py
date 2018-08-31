from __future__ import print_function

import sys
from operator import add, concat
from pyspark import SparkContext

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

import matplotlib.pyplot as plt

from math import sin, cos, sqrt, atan2, radians

### DEFINICAO DO POLIGONO SHENZHEN
shenzhen = Polygon([(22.734573, 113.763943),
                    (22.450680, 113.886438),
                    (22.604638, 114.414178),
                    (22.446780, 114.506558),
                    (22.506541, 114.628048),
                    (22.656394, 114.593470),
                    (22.805185, 114.346804),
                    (22.778334, 114.184327),
                    (22.659406, 114.175405),
                    (22.842981, 113.882361)])
def contains(x,y):
    try:
        float(x)
    except:
        return False

    try:
        float(y)
    except:
        return False
    
    X = float(x)
    Y = float(x)
    p=Point(X,Y)
    #print (shenzhen.contains(p))
    return shenzhen.contains(p)

def calcDist(x,y):
    R = 6373.0

    dlon = radians(y[1] - x[1])
    dlat = radians(y[0] - x[0])
    a = (sin(dlat/2))**2 + cos(radians(x[0])) * cos(radians(y[0])) * (sin(dlon/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = R * c
    ### RETORNA A DISTANCIA EM METROS
    return distance*1000

def calcAngle(x,y):
    R = 6373.0

    dlon = radians(y[1] - x[1])
    dlat = radians(y[0] - x[0])
    a = cos(radians(y[0]))*sin(dlon)
    b = cos(radians(x[0]))*sin(radians(y[0])) - sin(radians(x[0]))*cos(radians(y[0]))*cos(dlon)
    c = atan2(a,b)
    ### RETORNA A DISTANCIA EM METROS
    return c

def hourAssign(data):
    hours = [0 for x in range(24)]
    userold = '0'
    for i in data:
        if type(i)<>type(1.0): 
            hour = i.split(':')
            if len(hour)<>3:
                user = hour
                continue
            elif user <> userold: 
                hours[int(hour[0])]+=1
                userold = user
        else: continue
    return hours
            
def handoverAssign(x):
    handover = []
    for i in range(1,len(x)):
        if x[i-1][1][0][0]<>x[i][1][0][0]:
            handover.append((x[i-1][1][0][0],[x[i][0]]))
        else:
            continue #handover.append("NADA")
    return handover

def hourAssign2(data):
    hours = [[] for x in range(24)]
    userold='0'
    for i in data:
        if type(i)<>type(1.0): 
            hour = i.split(':')
            if len(hour)<>3:
                user = hour 
                continue
            elif user <> userold:
                hours[int(hour[0])].append(data[data.index(i)-1])
                userold = user
        else: continue
    return hours

def hourAssign3(data):
    hours = [0 for x in range(24)]
    for i in data:
        hour = i.split(':')
        hours[int(hour[0])]+=1
    return hours

if __name__ == "__main__":
    sc = SparkContext()
    ### SAO 38938 CELULAS REGISTRADAS EM SHENZHEN!
    ###out = open("shenzen-erbs.csv","w")
    
    counter = 0
    erbs = []
    for i in ["454.csv","455.csv","460.csv"]:
        f = open(i)
        f.readline()
        for j in f:
            line = j.strip('\n').split(",")
            p = Point(float(line[7]),float(line[6]))
            if shenzhen.contains(p):
                erbs.append(line)
                ###out.write(j)
                counter += 1
    print(counter)
    
    rddErbs = sc.parallelize(erbs)
    #print(rddErbs.count())

    coordErbs = rddErbs.map(lambda x: (x[4],float(x[7]),float(x[6]),float(x[8])))
    #print(coordErbs.take(10))

    rddPhone = sc.textFile("PhoneData.csv")
    rddPhoneStr = rddPhone.map(lambda x: x.strip('\n').split(",")).map(lambda x: (x[0],x[1],float(x[3]),float(x[2])))

    ### THIS RDD RETURNS THE THREE MOST HIGHEST BASE STATIONS TO AN USAR IN AN GIVEN SAMPLE
    ### SOMETHING LIKE: 
    # ((u'0055555805', u'00:03:50'), [('180442726', 72.00006720388618), ('180442725', 103.7307814419589), ('4422', 114.4415393637699)])
    ### ITS A KEY VALUE PAIR
    rddPhoneBase = rddPhoneStr.cartesian(coordErbs)\
                    .filter(lambda x: calcDist((x[0][2],x[0][3]),(x[1][1],x[1][2])) < (x[1][3]))\
                    .map(lambda x: ((x[0][0],x[0][1]),[(x[1][0],calcDist((x[0][2],x[0][3]),(x[1][1],x[1][2])))]))\
                    .reduceByKey(lambda x,y: sorted(x+y, key=lambda z: z[1])[:3])
                    #.reduceByKey(lambda x,y: min(x,y,key=lambda z: z[0][1]))
    #print(rddPhoneBase.take(10))

    rddBaseAccess = rddPhoneBase.flatMapValues(lambda x: x)\
                    .map(lambda x: (x[1][0],(x[0][0],x[0][1],x[1][1])))\
                    .reduceByKey(lambda x,y: x+y)\
                    .map(lambda x: (x[0],hourAssign(x[1])))
                    #.flatMapValues(hourAssign).

    #print(rddBaseAccess.take(10))
    rddBaseAccess2 = rddPhoneBase.flatMapValues(lambda x: x)\
                    .map(lambda x: (x[1][0],(x[0][0],x[0][1],x[1][1])))\
                    .reduceByKey(lambda x,y: x+y)\
                    .map(lambda x: (x[0],hourAssign2(x[1])))
                    #.flatMapValues(hourAssign).

    #print(rddBaseAccess.take(10))

    
    rddPhoneBase2 = rddPhoneBase.map(lambda x: (x[0][0],[(x[0][1],x[1])]))\
                    .reduceByKey(lambda x,y: sorted(x+y, key=lambda z: z[0].strip(":")))#concat)\
                    #.values()#.keys()#.flatMapValues(lambda x: x)#min(x,key=lambda z: z[0].split(":")[0]))
    #print(rddPhoneBase2.take(10))

    rddHandover = rddPhoneBase2.values().map(lambda x: handoverAssign(x)).coalesce(1)\
                 .flatMap(lambda x: x).reduceByKey(lambda x,y: x+y)\
                 .map(lambda x: (x[0],hourAssign3(x[1])))
    #print(rddHandover.take(10))

    
    img = plt.imread("map-shenzhen.png")
    fig, ax = plt.subplots()
    ax.imshow(img, extent=[0,671,0,361])

    zero_point = [22.428111, 113.724586]
    endx = [22.423773, 114.650691]
    xaxis = calcDist(zero_point, endx)
    endy = [22.871618, 113.715715]
    yaxis = calcDist(zero_point, endy)

    rddcoord = coordErbs.map(lambda x: (x[0],(x[1],x[2],x[3])))
    
    if sys.argv[1]=='1':
        final=rddHandover.collect()
        for i in final:
            coord = rddcoord.lookup(i[0])[0]

            r = calcDist(zero_point, coord[:2])
            alpha = calcAngle(zero_point, coord[:2])
            xcoord = r*cos(alpha)*671/xaxis
            ycoord = r*sin(alpha)*371/yaxis
            plt.plot(ycoord, xcoord, marker='o', markersize=coord[2]/200.0, color='red', alpha=0.5) #, markeredgecolor='none')

    elif sys.argv[1]=='2':
        final = rddBaseAccess.collect()
        for i in final:
            print(i)
            coord = rddcoord.lookup(i[0])[0]

            r = calcDist(zero_point, coord[:2])
            alpha = calcAngle(zero_point, coord[:2])
            xcoord = r*cos(alpha)*671/xaxis
            ycoord = r*sin(alpha)*371/yaxis
            plt.plot(ycoord, xcoord, marker='o', markersize=sum(i[1])/10.0 + 2, color='red', alpha=0.5) #, markeredgecolor='none')

    elif sys.argv[1]=='3':
        final=rddHandover.collect()
        for i in final:
            coord = rddcoord.lookup(i[0])[0]

            r = calcDist(zero_point, coord[:2])
            alpha = calcAngle(zero_point, coord[:2])
            xcoord = r*cos(alpha)*671/xaxis
            ycoord = r*sin(alpha)*371/yaxis
            plt.plot(ycoord, xcoord, marker='o', markersize=sum(i[1])/2.0 + 2, color='red', alpha=0.5) #, markeredgecolor='none')

        

    elif sys.argv[1]=='0':
        #final=rddcoord.collect()
        for i in erbs:
            r = calcDist(zero_point, [float(i[7]), float(i[6])])
            alpha = calcAngle(zero_point,  [float(i[7]), float(i[6])])
            xcoord = r*cos(alpha)*671/xaxis
            ycoord = r*sin(alpha)*371/yaxis
            plt.plot(ycoord, xcoord, marker='o', markersize=float(i[8])/200.0, color='red', alpha=0.5) #, markeredgecolor='none')

    elif sys.argv[1]=='4':
        erbs = []
        final = rddPhoneStr.collect() 
        for i in final:
            r = calcDist(zero_point, [float(i[2]), float(i[3])])
            alpha = calcAngle(zero_point,  [float(i[2]), float(i[3])])
            xcoord = r*cos(alpha)*671/xaxis
            ycoord = r*sin(alpha)*371/yaxis
            plt.plot(ycoord, xcoord, marker='o', markersize=2.0, color='black', alpha=0.5) #, markeredgecolor='none')
            

    #print(xcoord,ycoord)
    plt.savefig(sys.argv[1]+"-teste.png")  
    '''
    rddTaxi = sc.textFile("PhoneData.csv")
    rddTaxiStr = rddTaxi.map(lambda x: x.strip('\n').split(",")).map(lambda x: (x[0],x[1],float(x[3]),float(x[2])))\
                    .cartesian(coordErbs)\
                    .filter(lambda x: calcDist((x[0][2],x[0][3]),(x[1][1],x[1][2])) < (x[1][3]))\
                    .map(lambda x: ((x[0][0],x[0][1]),[(x[1][0],calcDist((x[0][2],x[0][3]),(x[1][1],x[1][2])))]))\
                    .reduceByKey(lambda x,y: sorted(x+y, key=lambda z: z[1])[:3])\
                    .map(lambda x: (x[0][0],[(x[0][1],x[1])]))\
                    .reduceByKey(lambda x,y: sorted(x+y, key=lambda z: z[0].strip(":")))
    '''
