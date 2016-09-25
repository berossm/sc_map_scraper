#! /usr/bin/python

import subprocess
import time
import datetime
import sys

if len(sys.argv) < 2 or len(sys.argv) > 3:
    print "Usage: starmap_all.py database_name [table_prefix]"
    exit(0)

now = datetime.datetime.now()
timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

with open("starmap-scraper-" + timestamp + ".log", "w+") as output:
    output.write("Starting scraper Log: " + timestamp + "\n")
    output.write("----------------------------------------\n")
    output.write("Running starmap_scraper.py\n")
    output.write("----------------------------------------\n")
    output.flush()
    #subprocess.call(["python", "./starmap_scraper.py+], stdout=output);
    output.write("\n");
    output.write("----------------------------------------\n")
    output.write("Running starmap_json_to_sql.py\n")
    output.write("----------------------------------------\n")
    output.flush()
    if len(sys.argv) == 2:
        subprocess.call(["python", "./starmap_json_to_sql.py ", \
                     sys.argv[1]], stdout=output);
    else:
        subprocess.call(["python", "./starmap_json_to_sql.py ", \
                     sys.argv[1], sys.argv[2]], stdout=output)
