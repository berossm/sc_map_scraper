#! /usr/bin/python

import subprocess
import time
import datetime

now = datetime.datetime.now()
formatted_now = now.strftime("%Y-%m-%d-%H-%M")
print formatted_now

with open("scraper.log", "w+") as output:
    output.write("Starting scraper Log: " 
    #subprocess.call(["python", "./starmap_scraper.py"], stdout=output);
