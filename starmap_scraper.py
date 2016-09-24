#! /usr/bin/python

import urllib2
import json
import os
base_url = "https://robertsspaceindustries.com/api/starmap"
base_path = "./starmap"
data_path = "/"
data_file = "bootup"
req = urllib2.Request(base_url + data_path + data_file, '')
response = urllib2.urlopen(req)
json_str = response.read()
json_data = json.loads(json_str)

if not os.path.exists(base_path):
  os.makedirs(base_path)

with open(base_path + data_path + data_file + ".json", 'w') as outfile:
   json.dump(json_data, outfile)

data_path = "/star-systems/"
if not os.path.exists(base_path + data_path):
  os.makedirs(base_path + data_path)

star_systems = json_data["data"]["systems"]["resultset"]

for system in star_systems:
  data_path = "/star-systems/"
  print "Fetching " + str(system["code"])
  req = urllib2.Request(base_url + data_path + system["code"], '')
  response = urllib2.urlopen(req)
  json_str = response.read()
  json_data = json.loads(json_str)
  if json_data["success"] != 1:
    print "...... FAILED."
  else: 
    with open(base_path + data_path + system["code"] + ".json", 'w') as outfile:
      json.dump(json_data, outfile)
    system_data = json_data["data"]["resultset"][0]["celestial_objects"]
    for celestial_object in system_data:
      data_path = "/celestial-objects/"
      if not os.path.exists(base_path + data_path):
        os.makedirs(base_path + data_path)
      
      print "--- Fetching " + str(celestial_object["code"])
      req = urllib2.Request(base_url + data_path +\
            celestial_object["code"], '')
      response = urllib2.urlopen(req)
      json_str = response.read()
      json_data = json.loads(json_str)
      if json_data["success"] != 1:
        print "    ...... FAILED."
      else:
        with open(base_path + data_path + celestial_object["code"] + ".json", 'w')\
              as outfile:
          json.dump(json_data, outfile)
