#! /usr/bin/python

import os
import sys
import MySQLdb
import json
base_path = "./starmap"
sql_path = "./sql"
data_file = "bootup"

if len(sys.argv) < 2 or len(sys.argv) > 3:
    print "Usage: starmap_all.py database_name [table_prefix]"
    exit(0)

if not os.path.exists(base_path):
    print "Run with scraper_all.py or run scraper_json_to_sql.py" + \
          "to generate proper json data"

if not os.path.exists(sql_path):
    os.makedirs(sql_path)

with open(base_path + "/" + data_file + ".json", 'r') as json_file:
    base_data = json.load(json_file)

if len(sys.argv) == 2:
    table_base = "`" + sys.argv[1] + "`.`"
else:
    table_base = "`" + sys.argv[1] + "`.`" + sys.argv[2]

with open(sql_path + "/affiliation.sql", 'w') as sql_file:
    sql_table = table_base + "affiliations`"
    sql_str = "CREATE TABLE " + sql_table + " ( `id` INT NOT NULL " +\
              "AUTO_INCREMENT , `code` VARCHAR(8) NOT NULL , `name` " +\
              "VARCHAR(64) NOT NULL , `color` VARCHAR(8) NOT NULL , " +\
              "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
    sql_file.write(sql_str)
    sql_str = "INSERT INTO " + sql_table + \
              " (`id`, `code`, `name`, `color`) VALUES "
    sql_rows = "('"
    for affiliation in base_data["data"]["affiliations"]["resultset"]:
        sql_rows += MySQLdb.escape_string(affiliation["id"])
        sql_rows += "', '" + MySQLdb.escape_string(affiliation["code"])
        sql_rows += "', '" + MySQLdb.escape_string(affiliation["name"])
        sql_rows += "', '" + MySQLdb.escape_string(affiliation["name"])[-6:]
        sql_rows += "), ('"
    sql_str += sql_rows[:-3] + ";\n"
    sql_file.write(sql_str)

with open(sql_path + "/systems.sql", 'w') as sql_file:
    sql_table = table_base + "systems`"
    sql_str = "CREATE TABLE " + sql_table + " ( `id` INT NOT NULL " +\
              "AUTO_INCREMENT , `code` VARCHAR(32) NOT NULL , `name` " +\
              "VARCHAR(128) NOT NULL , `description` TEXT NOT NULL , " +\
              "`type` VARCHAR(64) NOT NULL , `position_z` DOUBLE " +\
              "NOT NULL , `position_x` DOUBLE NOT NULL , `position_y` " +\
              "DOUBLE NOT NULL , `affiliation` INT NOT NULL , `status` " +\
              "VARCHAR(16) NOT NULL , `habitable_zone_inner` DOUBLE " +\
              "NOT NULL , `habitable_zone_outer` DOUBLE NOT NULL , " +\
              "`frost_line` DOUBLE NOT NULL , `aggregated_size` REAL " +\
              "NOT NULL , `aggregated_population` REAL NOT NULL , " +\
              "`aggregated_economy` REAL NOT NULL , `aggregated_danger` " +\
              "REAL NOT NULL , `time_modified` TIMESTAMP NOT NULL , " +\
              "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
    sql_file.write(sql_str)
    sql_str = "INSERT INTO " + sql_table + \
              " (`id`, `code`, `name`, `description`, `type`, " +\
              "`position_z`, `position_x`, `position_y`, `affiliation`, " +\
              "`status`, `habitable_zone_inner` , `habitable_zone_outer` , " +\
              "`frost_line` , `aggregated_size`, `aggregated_population`, " +\
              "`aggregated_economy`, `aggregated_danger`, `time_modified`) VALUES " 
    sql_rows = "(''"
    for system_file in os.listdir(base_path + "/star-systems/"):
        if system_file.endswith(".json"):
            with open(base_path + "/star-systems/"+system_file, 'r') as system_json:
                system_data = json.load(system_json)["data"]["resultset"][0]
                sql_rows += MySQLdb.escape_string(system_data["id"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["code"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["name"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["description"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["type"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["position_z"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["position_x"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["position_y"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["affiliation"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["status"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["habitable_zone_inner"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["habitable_zone_outer"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["frost_line"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["aggregated_size"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["aggregated_population"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["aggregated_economy"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["aggregated_danger"])
                sql_rows += "', '" + MySQLdb.escape_string(system_data["time_modified"])
                sql_rows += "), ('"
    sql_str += sql_rows[:-3] + ";\n"
    sql_file.write(sql_str)