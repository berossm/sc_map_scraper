#! /usr/bin/python

import os
import MySQLdb
base_path = "./starmap"
data_path = "/"
data_file = "base_data"

escaped_string = MySQLdb.escape_string("''\n")
print escaped_string
