# various hadoop functions for Data Exploration Notebook

import subprocess
import os
import pandas as pd
import re

def table_schema_from_pig(hcat_table_name):
    #returns schema of table with this database.name in hcatalog
    #   (pig-workaround as long as hcatweb api is not available...)
    pig_script = "table = LOAD '" + hcat_table_name + "' USING org.apache.hive.hcatalog.pig.HCatLoader(); DESCRIBE table;"
    
    p = subprocess.Popen(['pig','-useHCatalog','-x','mapreduce','-e', pig_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    
    pig_out = pd.DataFrame(out.split('\n'))
    pig_schema = pig_out[pig_out[0].str.contains('table: {')][0].iloc[0]
    
    #pig_schema = 'table: {number: chararray,category: chararray,open_time: chararray,priority_code: chararray,severity: chararray,updat_time: chararray,status: chararray,close_time: chararray,logical_name: chararray,downtime_start: chararray,brief_description: chararray}'        
    #print pig_schema
    
    colnames=(re.findall(r'(\b\w*\b):',pig_schema[8:-1]))
    coltypes=(re.findall(r':\s(\b\w*\b)',pig_schema[8:-1]))
    
    table_schema = {'columns':{}}
    
    col_sequence = 0
    for colname in colnames:
        table_schema['columns'][colname] = {'col_sequence': col_sequence, 'type':coltypes[col_sequence]}
        col_sequence += 1
    
    return table_schema
    
def table_schema_from_spark(hcat_table_name):
    #returns schema of table with this database.name in hcatalog
    #   (spark-workaround as long as hcatweb api is not available...)
    # initialize spark
    import findspark
    findspark.init()
     
    import pyspark
    from pyspark.sql import HiveContext
    
    sc_conf = pyspark.SparkConf()
    #sc_conf.set('spark.executor.extraClassPath','/opt/cloudera/parcels/CDH/lib/hive/lib/*')
    #sc_conf.set('spark.master','yarn-client')
    
    sc = pyspark.SparkContext(appName = 'ade_get_table_schema', conf=sc_conf)
    hc = HiveContext(sc)
    
    hive_schema = hc.table(hcat_table_name).schema.jsonValue()
    
    print hive_schema
    
    sc.stop()
    
    table_schema = {'columns':{}}
    
    col_sequence = 0
    for field in hive_schema['fields']:
        table_schema['columns'][field['name']] = {'col_sequence': col_sequence, 'type':field['type']}
        col_sequence += 1
    
    return table_schema