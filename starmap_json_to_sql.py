#! /usr/bin/python

import os
import sys
import MySQLdb
import json
from unidecode import unidecode


def str_or_none(json_object, key_str):
    if key_str in json_object.keys():
        json_data = json_object[key_str]
        if json_data == None:
            temp_str = "NULL"
        else:
            temp_str = unicode(json_data)
            ascii_str = unidecode(temp_str)
            temp_str = "'" + MySQLdb.escape_string(ascii_str) + "'"
    else:
        temp_str = "NULL"
    return temp_str

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
    sql_str = "CREATE TABLE " + sql_table + \
              " ( `id` INT NOT NULL AUTO_INCREMENT , " + \
              "`code` VARCHAR(9) NOT NULL , " + \
              "`name` VARCHAR(64) NOT NULL , " + \
              "`color` VARCHAR(8) NOT NULL , " + \
              "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
    sql_file.write(sql_str)
    sql_base = "INSERT INTO " + sql_table + \
               " (`id`, `code`, `name`, `color`) VALUES "    
    for affiliation in base_data["data"]["affiliations"]["resultset"]:
        sql_row = "(" + str_or_none(affiliation, "id")
        sql_row += ", " + str_or_none(affiliation, "code")
        sql_row += ", " + str_or_none(affiliation, "name")
        sql_row += ", " + str_or_none(affiliation, "color")
        sql_row += ")"
        sql_str = sql_base + sql_row + ";\n"
        sql_file.write(sql_str)

with open(sql_path + "/species.sql", 'w') as sql_file:
    sql_table = table_base + "species`"
    sql_str = "CREATE TABLE " + sql_table + \
              " ( `id` INT NOT NULL AUTO_INCREMENT , " + \
              "`code` VARCHAR(8) NOT NULL , " + \
              "`name` VARCHAR(64) NOT NULL , " + \
              "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
    sql_file.write(sql_str)
    sql_base = "INSERT INTO " + sql_table + \
               " (`id`, `code`, `name`) VALUES "    
    for species in base_data["data"]["species"]["resultset"]:
        sql_row = "(" + str_or_none(species, "id")
        sql_row += ", " + str_or_none(species, "code")
        sql_row += ", " + str_or_none(species, "name")
        sql_row += ")"
        sql_str = sql_base + sql_row + ";\n"
        sql_file.write(sql_str)

with open(sql_path + "/tunnels.sql", 'w') as sql_file:
    sql_table = table_base + "tunnels`"
    sql_str = "CREATE TABLE " + sql_table + \
              " ( `id` INT NOT NULL AUTO_INCREMENT , " + \
              "`name` VARCHAR(128) NULL, " + \
              "`size` VARCHAR(1) NOT NULL , " + \
              "`direction` VARCHAR(1) NOT NULL , " + \
              "`entry_id` INT NOT NULL , " + \
              "`exit_id` INT NOT NULL , " + \
              "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
    sql_file.write(sql_str)
    sql_base = "INSERT INTO " + sql_table + \
               " (`id`, `name`, `size`, `direction`, `entry_id`, `exit_id`) VALUES "
    for tunnel in base_data["data"]["tunnels"]["resultset"]:
        sql_row = "(" + str_or_none(tunnel, "id")
        sql_row += ", " + str_or_none(tunnel, "name")
        sql_row += ", " + str_or_none(tunnel, "size")
        sql_row += ", " + str_or_none(tunnel, "direction")
        sql_row += ", " + str_or_none(tunnel, "entry_id")
        sql_row += ", " + str_or_none(tunnel, "exit_id")
        sql_row += ")"
        sql_str = sql_base + sql_row + ";\n"
        sql_file.write(sql_str)

with open(sql_path + "/systems.sql", 'w') as sql_file:
    sql_table = table_base + "systems`"
    sql_str = "CREATE TABLE " + sql_table + " ( " + \
              "`id` INT NOT NULL AUTO_INCREMENT , " + \
              "`code` VARCHAR(32) NOT NULL , " + \
              "`name` VARCHAR(128) NOT NULL , "+ \
              "`description` TEXT NOT NULL , " + \
              "`type` VARCHAR(64) NOT NULL , " + \
              "`position_z` DOUBLE NOT NULL , " + \
              "`position_x` DOUBLE NOT NULL , " + \
              "`position_y` DOUBLE NOT NULL , " + \
              "`affiliation` INT NOT NULL , " + \
              "`status` VARCHAR(16) NOT NULL , " + \
              "`habitable_zone_inner` DOUBLE NULL , " + \
              "`habitable_zone_outer` DOUBLE NULL , " + \
              "`frost_line` DOUBLE NULL , " + \
              "`aggregated_size` REAL NULL , " + \
              "`aggregated_population` REAL NULL , " + \
              "`aggregated_economy` REAL NULL , " + \
              "`aggregated_danger` REAL NULL , " + \
              "`time_modified` TIMESTAMP NOT NULL , " + \
              "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
    sql_file.write(sql_str)
    sql_base = "INSERT INTO " + sql_table + \
               " (`id`, `code`, `name`, `description`, `type`, " + \
               "`position_z`, `position_x`, `position_y`, " + \
               "`affiliation`, `status`, `habitable_zone_inner` , " + \
               "`habitable_zone_outer` , `frost_line` , " + \
               "`aggregated_size`, `aggregated_population`, " + \
               "`aggregated_economy`, `aggregated_danger`, " + \
               "`time_modified`) VALUES "     
    for system_file in os.listdir(base_path + "/star-systems/"):
        if system_file.endswith(".json"):
            with open(base_path + "/star-systems/"+system_file, 'r') as system_json:
                system_data = json.load(system_json)["data"]["resultset"][0]
                sql_row = "(" + str_or_none(system_data,"id")
                sql_row += ", " + str_or_none(system_data, "code")
                sql_row += ", " + str_or_none(system_data, "name")
                sql_row += ", " + str_or_none(system_data, "description")
                sql_row += ", " + str_or_none(system_data, "type")
                sql_row += ", " + str_or_none(system_data, "position_z")
                sql_row += ", " + str_or_none(system_data, "position_x")
                sql_row += ", " + str_or_none(system_data, "position_y")
                #TODO: Eveluate if affiliation array can be more than 1
                sql_row += ", " + str_or_none(system_data["affiliation"][0], "id")
                sql_row += ", " + str_or_none(system_data, "status")
                sql_row += ", " + str_or_none(system_data, "habitable_zone_inner")
                sql_row += ", " + str_or_none(system_data, "habitable_zone_outer")
                sql_row += ", " + str_or_none(system_data, "frost_line")
                sql_row += ", " + str_or_none(system_data, "aggregated_size")
                sql_row += ", " + str_or_none(system_data, "aggregated_population")
                sql_row += ", " + str_or_none(system_data, "aggregated_economy")
                sql_row += ", " + str_or_none(system_data, "aggregated_danger")
                sql_row += ", " + str_or_none(system_data, "time_modified")
                sql_row += ")"
                sql_str = sql_base + sql_row + ";\n"
                sql_file.write(sql_str)

sub_type_file = open(sql_path + "/sub_types.sql", 'w')
sub_type_table = table_base + "sub_types`"
sub_type_str = "CREATE TABLE " + sub_type_table + " ( " + \
               "`id` INT NOT NULL , " + \
               "`type` VARCHAR(256) NOT NULL , " + \
               "`name` VARCHAR(256) NOT NULL , " + \
               "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
sub_type_file.write(sub_type_str)

populations_file = open(sql_path + "/populations.sql", 'w')
populations_table = table_base + "populations`"
populations_str = "CREATE TABLE " + populations_table + " ( " + \
                  "`id` int(11) NOT NULL AUTO_INCREMENT, " + \
                  "`object_id` int(11) NOT NULL, " + \
                  "`species_id` int(11) NOT NULL, " + \
                  "`population` double NOT NULL, " + \
                  "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
populations_file.write(populations_str)

children_file = open(sql_path + "/children.sql", 'w')
children_table = table_base + "children`"
children_str = "CREATE TABLE " + children_table + " ( " + \
               "`id` int(11) NOT NULL, " + \
               "`parrent_id` int(11) NOT NULL, " + \
               "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
children_file.write(children_str)

with open(sql_path + "/celestial_objects.sql", 'w') as celestial_objects_file:
    celestial_objects_table = table_base + "celestial_objects`"
    celestial_objects_str = "CREATE TABLE " + celestial_objects_table + " ( " + \
                            "`id` INT NOT NULL AUTO_INCREMENT , " + \
                            "`code` VARCHAR(256) NOT NULL , " + \
                            "`type` VARCHAR(256) NOT NULL , " + \
                            "`name` VARCHAR(256) NULL , " + \
                            "`designation` VARCHAR(256) NULL , " + \
                            "`description` TEXT NULL , " + \
                            "`affiliation` INT NULL , " + \
                            "`longitude` FLOAT NULL , " + \
                            "`latitude` FLOAT NULL , " + \
                            "`distance` FLOAT NULL , " + \
                            "`axial_tilt` FLOAT NULL , " + \
                            "`size` FLOAT NULL , " + \
                            "`orbit_period` FLOAT NULL , " + \
                            "`age` FLOAT NULL , " + \
                            "`parent_id` INT NULL , " + \
                            "`subtype_id` INT NULL , " + \
                            "`habitable` INT NULL , " + \
                            "`fairchanceact` INT NULL , " + \
                            "`sensor_danger` INT NULL , " + \
                            "`sensor_population` INT NULL , " + \
                            "`sensor_economy` INT NULL , " + \
                            "`time_modified` TIMESTAMP NOT NULL , " + \
                            "PRIMARY KEY (`id`)) ENGINE = InnoDB;\n"
    celestial_objects_file.write(celestial_objects_str)
    celestial_objects_base = \
               "INSERT INTO " + celestial_objects_table + \
               " (`id`, `code`, `type`, `name`, `designation`, " + \
               "`description`, `affiliation`, " + \
               "`longitude`, `latitude`, `distance` , `axial_tilt`, " + \
               "`size` , `orbit_period` , `age`,  " + \
               "`parent_id`, `subtype_id`, " + \
               "`habitable`, `fairchanceact`, " + \
               "`sensor_danger`, `sensor_population`, `sensor_economy`, " + \
               "`time_modified`) VALUES "
    children = {}
    populations = {}
    sub_type = {}
    for celestial_objects_src in os.listdir(base_path + "/celestial-objects/"):
        if celestial_objects_src.endswith(".json"):
            with open(base_path + "/celestial-objects/" + celestial_objects_src, 'r') as object_json:
                object_data = json.load(object_json)["data"]["resultset"][0]
                sql_row = "(" + str_or_none(object_data, "id")
                sql_row += ", " + str_or_none(object_data, "code")
                sql_row += ", " + str_or_none(object_data, "type")
                sql_row += ", " + str_or_none(object_data, "name")
                sql_row += ", " + str_or_none(object_data, "designation")
                sql_row += ", " + str_or_none(object_data, "description")
                #TODO: Eveluate if affiliation array can be more than 1
                if len(object_data["affiliation"]) == 0:
                    sql_row += ", NULL"
                else:
                    sql_row += ", " + str_or_none(object_data["affiliation"][0], "id")            
                sql_row += ", " + str_or_none(object_data, "longitude")
                sql_row += ", " + str_or_none(object_data, "latitude")
                sql_row += ", " + str_or_none(object_data, "distance")
                sql_row += ", " + str_or_none(object_data, "axial_tilt")
                sql_row += ", " + str_or_none(object_data, "size")
                sql_row += ", " + str_or_none(object_data, "orbit_period")
                sql_row += ", " + str_or_none(object_data, "age")
                sql_row += ", " + str_or_none(object_data, "parent_id")
                if object_data["parent_id"] != None:
                    children[int(object_data["id"])] = int(object_data["parent_id"])
                #TODO: Look at making this a function?
                if object_data["subtype_id"] == None:
                    sql_row += ", NULL"
                else:
                    sql_row += ", " + str_or_none(object_data["subtype"], "id")
                    sub_type[int(object_data["subtype"]["id"])] = \
                        {'type' : unidecode(object_data["subtype"]["type"]), \
                        'name' : unidecode(object_data["subtype"]["name"]) }
                sql_row += ", " + str_or_none(object_data, "habitable")
                sql_row += ", " + str_or_none(object_data, "fairchanceact")
                sql_row += ", " + str_or_none(object_data, "sensor_danger")
                sql_row += ", " + str_or_none(object_data, "sensor_population")
                sql_row += ", " + str_or_none(object_data, "sensor_economy")
                sql_row += ", " + str_or_none(object_data, "time_modified")
                sql_row += ");\n"
                sql_str = celestial_objects_base + sql_row
                celestial_objects_file.write(sql_str)
                #TODO: Process population when any example is available from json data

sub_type_base = "INSERT INTO " + sub_type_table + \
                " (`id`, `type`, `name` ) VALUES "
for index in sub_type:
    sub_type_row = "('" + str(index) + "', '"
    sub_type_row += sub_type[index]['type'] + "', '"
    sub_type_row += sub_type[index]['name'] + "');\n"
    sub_type_file.write(sub_type_base + sub_type_row)
sub_type_file.close()

children_base = "INSERT INTO " + children_table + \
                " (`id`, `parrent_id` ) VALUES "
for index in children:
    child_row = "('" + str(index) + "', '"
    child_row += str(children[index]) + "');\n"
    children_file.write(children_base + child_row)
children_file.close()

#TODO: Run populations insert - duplicates allowed
populations_file.close()