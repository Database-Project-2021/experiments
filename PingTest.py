import sys
import re
import subprocess
#import matplotlib.pyplot as plt
from argparse import ArgumentParser

pings = []

def initialize():
    parser = ArgumentParser()

    parser.add_argument('destination', type=str)
    parser.add_argument( '-i', '--interval', type = float, default = 1)
    args = parser.parse_args()
    return args

def Output():
    print("Stop Recording Pings")
    print("Generate Output File...")
    outFile = "Ping.csv"
    with open(outFile, 'w') as outFile:
        for ping in pings:
            outFile.write(str(ping))
            outFile.write("\n")
    Xaxis = range(0,len(pings),Args.interval)
    #plt.plot(Xaxis, pings)
    #plt.xlabel('Time(s)')
    #plt.ylabel('Ping(ms)')
    #plt.savefig("Ping.png")
    print("Bye")

def Ping(ipAddress, interval = 1):
    print("Start Recording Pings...")
    if(interval == 1):
        p = subprocess.Popen(['ping', ipAddress], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        p = subprocess.Popen(['ping', '-i %f' % (interval), ipAddress], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while(p.poll() == None):
        out = p.stdout.readline().decode("utf-8", "replace")
        #print(out)
        ping = re.search(r'\d+\D\d+\s\bms\b', out)
        if(ping != None):
            #print(ping.group())
            ping = ping.group()
            ping = re.search(r'\d+\D\d+', ping).group()
            pings.append(float(ping))
            #print(numbers)
        #print(ping)
    #print("The Remote entry is down")
    #Output()

Args = initialize()
try:
    Ping(Args.destination, Args.interval)
except KeyboardInterrupt:
    Output()
    sys.exit()





