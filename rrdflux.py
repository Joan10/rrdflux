#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys,getopt
import re
import os
import rrdtool
import xml.etree.ElementTree as ET
import pprint
from influxdb import InfluxDBClient


def main(argv):

   RRD_MIN_RES=300

   update=False
   dump=False
   fname=""
   host="localhost"
   port="8086"
   db=""
   key=""
   user=""
   password=""
   device=""

   def help():
      print('Usage: rddflux.py [-u|-m] -f <RRD FILE> [-H <INFLUXDB HOST>] [-p <INFLUXDB PORT>] -d DATABASE [-U user] [-P password] [-k KEY] -D device [-h] ')
      print('Updates or dumps passed RRD File to selected InfluxDB database')
      print('	-h, --help		Display help and exit')
      print('	-u, --update		Only update database with last value')
      print('	-m, --dump		Dump full RRD to database')
      print('	-f, --file		RRD file to dump')
      print('	-H, --host		Optional. Name or IP of InfluxDB server. Default localhost.')
      print('	-p, --port		Optional. InfluxDB server port. Default 8086.')
      print('	-d, --database		Database name where to store data.')
      print('	-U, --user		Optional. Database user.')
      print('	-P, --password		Optional. Database password.')
      print('	-k, --key		Optional. Key used to store data values. Taken from RRD file\'s name if not specified.')
      print('	-D, --device		Device the RRD metrics are related with.')
   try:
      opts, args = getopt.getopt(argv,"humf:H:p:d:U:P:k:D:",["help=","update=","dump=","file=","host=","port=","database=","user=","password=","key=","device="])
   except getopt.GetoptError:
      help()
      sys.exit(2)

   for opt, arg in opts:
      if opt == '-h':
         help()
         sys.exit()
      elif opt in ("-u", "--update"):
         update = True
      elif opt in ("-m", "--dump"):
         dump = True
      elif opt in ("-f", "--file"):
         fname = arg
      elif opt in ("-H", "--host"):
         host = arg
      elif opt in ("-p", "--port"):
         port = arg
      elif opt in ("-d", "--database"):
         db = arg
      elif opt in ("-U", "--user"):
         user = arg
      elif opt in ("-P", "--password"):
         password = arg
      elif opt in ("-k", "--key"):
         key = arg
      elif opt in ("-D", "--device"):
         device = arg

   if device == "" or fname == "" or db == "" or (update == False and dump == False) or (update == True and dump == True):
      print("ERROR: Missing or duplicated parameters.")
      help()
      sys.exit(2)

   client = InfluxDBClient(host, port, user, password, db)
   client.query("create database "+db+";") # Create database if it not exists
   
   if key == "":
      key = re.sub('\.rrd$','',os.path.split(fname)[1])
  
   if update == True:
      # We save the last two records of the rrd tool to avoid missing data 
      lastvalue = rrdtool.fetch(fname,"AVERAGE",'-s', str(rrdtool.last(fname)-2*RRD_MIN_RES),
                                                '-e', str(rrdtool.last(fname)-RRD_MIN_RES),'-r', str(RRD_MIN_RES))
      unixts=lastvalue[0][1]
      val=lastvalue[2][0][0]
      json_body = [
         {
            "measurement": device,
            "time": unixts,
            "fields": {
                key: val,
            }
         }
      ]
      client.write_points(json_body)

      unixts=lastvalue[0][1]-RRD_MIN_RES
      val=lastvalue[2][0][0]
      json_body = [
         {
            "measurement": device,
            "time": unixts,
            "fields": {
                key: val,
            }
         }
      ]
      client.write_points(json_body)


   if dump == True:
      allvalues = rrdtool.fetch(
         fname,
         "AVERAGE",
         '-e', str(rrdtool.last(fname)-RRD_MIN_RES),
         '-r', str(RRD_MIN_RES))
      i=0
      while i < len(allvalues[2]):
         val=allvalues[2][i][0]
         unixts=allvalues[0][0]+(i+1)*RRD_MIN_RES
         json_body = [
            {
               "measurement": device,
               "time": unixts,
               "fields": {
                   key: val,
               }
            }
         ]
         client.write_points(json_body)
         i=i+1


if __name__ == "__main__":
   main(sys.argv[1:])


