from datetime import datetime, timedelta
import time

def sleepUntil(wakeTime):
    while datetime.now()< wakeTime:
        time.sleep(1)
    return

def wakeAtInterval(interval):
    if not isinstance(interval, timedelta):
        print('wake interval must be timedelta')
    wakeTime= datetime.now().replace(second=0)+ interval
    sleepUntil(wakeTime)
    return


def findNextIntervalMinute(intervalMinute):
    time= datetime.now().replace(minute= 0)
    time= time.replace(second=0)

    while (time< datetime.now()):
        time= time+ timedelta(minutes= intervalMinute)
    return time


def setTimeAtMinute(minute):
    targetTime= datetime.now().replace(minute=0) #start from hour
    targetTime= targetTime.replace(second=0)

    targetTime= targetTime+ timedelta(minutes= minute)

    while ( targetTime< datetime.now() ):
        targetTime= datetime.now()+ timedelta(hours=1)
    return targetTime


if __name__== '__main__':
    testTime= datetime.now()+ timedelta(minutes=1) #test time at next minute

    triggerTime= setTimeAtMinute(testTime.minute)
    print('wake up at:', triggerTime)
    sleepUntil(triggerTime)

    i=0
    while (i<10):
        print(i, datetime.now())
        wakeAtInterval(timedelta(seconds=5))
        i+=1
