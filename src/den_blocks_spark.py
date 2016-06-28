block_conf = {'mapping':{
                         'table':'b_table'
                         ,'column':'b_column'
                         ,'singlevalue':'b_column'
                         ,'time':'b_column_time'
                         ,'category':'b_column_cat'
                         ,'category_many':'b_column_cat_many'
                        }
              ,'blocks':{
                        'b_nb_header':{'description':'block for header of notebook'
                                        , 'views':{
                                                   '1':'v_nb_header'
                                                   }
                                        }
                        ,'b_nb_footer':{'description':'block for footer of notebook'
                                        , 'views':{
                                                   '1':'v_nb_footer'
                                                   }
                                        }
                        ,'b_table':{'description':'block for tables'
                                 ,'views':{
                                           '1':'v_table_header'
                                           }
                                 }
                        ,'b_column':{'description':'default block for columns'
                                     ,'views':{
                                               '1':'v_column_header'
                                               ,'2':'v_column_counts'
                                               }
                                     }
                        ,'b_column_time':{'description':'block for columns with times'
                                          ,'views':{
                                               '1':'v_column_header'
                                               ,'2':'v_column_counts'
                                               ,'3':'v_column_time'
                                               }
                                     }
                        ,'b_column_cat':{'description':'block for columns with categories'
                                         ,'views':{
                                               '1':'v_column_header'
                                               ,'2':'v_column_counts'
                                               ,'3':'v_column_cat'
                                               }
                                         }
                        ,'b_column_cat_many':{'description':'block for columns with many categories'
                                         ,'views':{
                                               '1':'v_column_header'
                                               ,'2':'v_column_counts'
                                               ,'3':'v_column_cat_freq'
                                               ,'4':'v_column_cat_many'                                               }
                                         }
                        }
              ,'views':{
                        'v_nb_header':{'description':'header for notebooks',
                                          'cells':{
                                                   '1':'c_nb_header'
                                                   ,'2':'c_nb_python'
                                                   ,'3':'c_nb_spark'}
                                          }
                        ,'v_nb_footer':{'description':'footer for notebooks',
                                          'cells':{
                                                   '1':'c_nb_footer'
                                                   }
                                          }
                        ,'v_table_header':{'description':'header for tables',
                                          'cells':{
                                                   '1':'c_table_header'
                                                   ,'2':'c_table_load'
                                                   ,'3':'c_table_notes'
                                                   }
                                          }
                        ,'v_column_header':{'description':'header for columns',
                                            'cells':{
                                                     '1':'c_column_header'
                                                     ,'2':'c_column_info'
                                                     }
                                            }
                        ,'v_column_counts':{'description':'view with basic counts'
                                            ,'cells':{
                                                      '1':'c_column_counts_header'
                                                      ,'2':'c_column_counts'
                                                      ,'3':'c_column_counts_results'
                                                      ,'4':'c_view_notes'
                                                      }
                                            }
                        ,'v_column_time':{'description':'view with time counts'
                                            ,'cells':{
                                                      '1':'c_column_time_header'
                                                      ,'2':'c_column_time'
                                                      ,'3':'c_column_time_results'
                                                      ,'4':'c_view_notes'
                                                      }
                                            }
                        ,'v_column_cat':{'description':'view with category counts'
                                            ,'cells':{
                                                      '1':'c_column_cat_header'
                                                      ,'2':'c_column_cat'
                                                      ,'3':'c_column_cat_results'
                                                      ,'4':'c_view_notes'
                                                      }
                                            }
                        ,'v_column_cat_many':{'description':'view with category (many categories) counts'
                                            ,'cells':{
                                                      '1':'c_column_cat_header'
                                                      ,'2':'c_column_cat'
                                                      ,'3':'c_column_cat_many_results'
                                                      ,'4':'c_view_notes'
                                                      }
                                            }
                        ,'v_column_cat_freq':{'description':'view with category frequency counts'
                                            ,'cells':{
                                                      '1':'c_column_cat_freq_header'
                                                      ,'2':'c_column_cat_freq'
                                                      ,'3':'c_column_cat_freq_results'
                                                      ,'4':'c_view_notes'
                                                      }
                                            }
                        }
              ,'cells':{
                        'c_nb_header':{'description':'markdown cell with header of notebook'
                                          ,'source':'# Data Exploration Notebook'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_nb_footer':{'description':'code cell with footer of notebook'
                                          ,'source':'table_rdf.unpersist() \nsc.stop()'
                                          ,'nb_cell_type':'code'}
                        ,'c_nb_python':{'description':'python cell with imports and functions'
                                          ,'source':'get_source_c_nb_python'
                                          ,'nb_cell_type':'code'}
                        ,'c_nb_spark':{'description':'python cell for initializing spark context'
                                          ,'source':'get_source_c_nb_spark'
                                          ,'nb_cell_type':'code'}
                        ,'c_table_header':{'description':'markdown cell with header for tables'
                                          ,'source':'get_source_c_table_header'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_table_load':{'description':'code cell to load table'
                                          ,'source':'get_source_c_table_load'
                                          ,'nb_cell_type':'code'}
                        ,'c_table_notes':{'description':'markdown cell for notes on this table'
                                          ,'source':'#### Notes \n none'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_header':{'description':'markdown cell with header for columns'
                                          ,'source':'get_source_c_column_header'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_info':{'description':'markdown cell general information on columns'
                                          ,'source':'get_source_c_column_info'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_counts_header':{'description':'markdown cell with header for column counts'
                                          ,'source':'### Basic column counts'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_counts':{'description':'code cell for getting column counts with spark'
                                          ,'source':'get_source_c_column_counts'
                                          ,'nb_cell_type':'code'}
                        ,'c_column_counts_results':{'description':'code cell viewing basic column count results'
                                          ,'source':'get_source_c_column_counts_results'
                                          ,'nb_cell_type':'code'}
                        ,'c_view_notes':{'description':'markdown cell for notes on this view'
                                          ,'source':'#### Notes \n none'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_time_header':{'description':'markdown cell with header for time counts'
                                          ,'source':'### Time counts'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_time':{'description':'code cell for getting time counts'
                                          ,'source':'get_source_c_time_counts'
                                          ,'nb_cell_type':'code'}
                        ,'c_column_time_results':{'description':'code cell for viewing time count results'
                                          ,'source':'get_source_c_time_counts_results'
                                          ,'nb_cell_type':'code'}
                        ,'c_column_cat_header':{'description':'markdown cell with header for category counts'
                                          ,'source':'### Counts per category'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_cat':{'description':'code cell for getting category counts'
                                          ,'source':'get_source_c_cat_counts'
                                          ,'nb_cell_type':'code'}
                        ,'c_column_cat_results':{'description':'code cell for viewing category count results'
                                          ,'source':'get_source_c_cat_counts_results'
                                          ,'nb_cell_type':'code'}
                        ,'c_column_cat_many_results':{'description':'code cell for viewing category count results for many categories'
                                          ,'source':'get_source_c_cat_many_counts_results'
                                          ,'nb_cell_type':'code'}
                        ,'c_column_cat_freq_header':{'description':'markdown cell with header for frequency counts of categories'
                                          ,'source':'### Frequency counts per category'
                                          ,'nb_cell_type':'markdown'}
                        ,'c_column_cat_freq':{'description':'code cell for getting frequency category counts'
                                          ,'source':'get_source_c_cat_freq_counts'
                                          ,'nb_cell_type':'code'}
                        ,'c_column_cat_freq_results':{'description':'code cell for viewing frequency category count results'
                                          ,'source':'get_source_c_cat_freq_counts_results'
                                          ,'nb_cell_type':'code'}
                        }
              }

def get_source_c_nb_python(den_nb_conf,nb_cell):
    nb_name = den_nb_conf['filename'].split('.')[0]
    source = """#general imports and functions

import pickle
import os
import os.path, time
import pandas as pd

# initialize plotly
import plotly
from plotly.graph_objs import *
plotly.offline.init_notebook_mode()
"""
    return source

def get_source_c_nb_spark(den_nb_conf,nb_cell):
    nb_name = den_nb_conf['filename'].split('.')[0]
    source = """#initialize spark
import findspark
findspark.init()
 
import pyspark
from pyspark.sql import HiveContext
import pyspark.sql.functions as psf

sc_conf = pyspark.SparkConf() \\
    .setMaster('local[*]') \\
    .setAppName('""" + nb_name + """') #\\
#    .set('spark.driver.memory','6g') #\\
#    .set('spark.executor.memory','3g') #\\
#    .set('spark.serializer','org.apache.spark.serializer.KryoSerializer') #\\
#    .set('spark.executor.extraClassPath','/opt/cloudera/parcels/CDH/lib/hive/lib/*')

sc = pyspark.SparkContext(conf=sc_conf)
hc = HiveContext(sc)

print sc._conf.getAll()

# check whether folder for results exists
if not os.path.exists('./pkl'):
    os.makedirs('./pkl')
"""
    return source


def get_source_c_table_header(den_nb_conf,nb_cell):
    db = nb_cell['table'].split('.')[0]
    tbl = nb_cell['table'].split('.')[1]
    conf = den_nb_conf['filename'].split('.')[0] + '.den_nb_conf.json'
    source = '''# ''' + nb_cell['table'] +'''

Metastore manager: [sample data](http://lsrv2118.linux.rabobank.nl:8888/metastore/table/''' + db + '''/''' + tbl + '''/read)'''
# \n
# ADE configuration: open -t ''' + conf
    return source

def get_source_c_table_load(den_nb_conf,nb_cell):
    table = nb_cell['table']
    
    source = """#load hive table in sqlcontext and count rows

table_rdf = hc.table('""" + table + """').cache()

pkl_fname = './pkl/""" + table + """.TableCounts.pkl'

if not os.path.isfile(pkl_fname):
    nr_rows = table_rdf.count()

    with open(pkl_fname, 'wb') as f:
        pickle.dump(nr_rows, f)

with open(pkl_fname, 'rb') as f:
    nr_rows = pickle.load(f)
    
print 'nr of rows in table:', nr_rows
"""
    return source



def get_source_c_column_header(den_nb_conf,nb_cell):
    source = '***\n## ' + nb_cell['column']
    return source

def get_source_c_column_info(den_nb_conf,nb_cell):
    den_type = den_nb_conf['tables'][nb_cell['table']]['columns'][nb_cell['column']]['den_type']
    pattern = den_nb_conf['tables'][nb_cell['table']]['columns'][nb_cell['column']]['pattern']
    if pattern:
        pat = ' (format: ' + pattern +')'
    else:
        pat =''
    source = '''Table: ''' + nb_cell['table'] + '''\n
ADE type: ''' + den_type + pat
    return source



def get_source_c_column_counts(den_nb_conf,nb_cell):
    # WARNING: den_notebook.den_nb_auto_assign_col_types() depends on sequence/values in ColumnCounts.pkl !!!
    table = nb_cell['table']
    column = nb_cell['column']

    source = """#basic column counts: compute and save results

pkl_fname = './pkl/""" + table + """.""" + column + """.ColumnCounts.pkl'

if not os.path.isfile(pkl_fname):
    col_name = '""" + column + """'
    col_data_rdf = table_rdf.select(col_name).cache() 

    col_stats = {
        'count':{
            'desc':'Nr of values:         '
            ,'val':col_data_rdf.count()
        }
        ,'distinct':{
            'desc':'Nr of distinct values:'
            ,'val':col_data_rdf.distinct().count()
        }
        ,'nulls':{
            'desc':'Nr of null values:    '
            ,'val':col_data_rdf.filter(col_name + ' is null').count()
        }
        ,'empty':{
            'desc':'Nr of empty values:   '
            ,'val':col_data_rdf.filter(col_name + " = ''").count()
        }
        ,'min':{
            'desc':'Minimum value:        '
            ,'val':col_data_rdf.sort(col_name, ascending=True).first()[col_name]
        }
        ,'max':{
            'desc':'Maximum value:        '
            ,'val':col_data_rdf.sort(col_name, ascending=False).first()[col_name]
        }
    }
    col_data_rdf.unpersist()
    
    with open(pkl_fname, 'wb') as f:
        pickle.dump(col_stats, f)
"""
    return source

def get_source_c_column_counts_results(den_nb_conf,nb_cell):
    fn_results = './pkl/' + nb_cell['table'] + '.' + nb_cell['column'] + '.ColumnCounts.pkl'

    source = '''#basic column counts: load and show results 
print "date of results: %s" % time.ctime(os.path.getmtime("''' + fn_results + '''"))
print ""

col_stats = {}
with open("''' + fn_results + '''", 'rb') as f:
    col_stats = pickle.load(f)

stats = ['count','distinct','nulls','empty','min','max']

for stat in stats:
    print col_stats[stat]['desc'], col_stats[stat]['val']
'''
    return source



def get_source_c_time_counts(den_nb_conf,nb_cell):
    table = nb_cell['table']
    column = nb_cell['column']
    pattern = den_nb_conf['tables'][table]['columns'][column]['pattern']

    if pattern == 'timestamp':
        coldatardf = """col_data_rdf = table_rdf.select(psf.from_unixtime(psf.floor('""" + column + """'),format='yyyy-MM-dd HH').alias('col_time_agg'))"""
    else:
        coldatardf = """col_data_rdf = table_rdf.select(psf.date_format('""" + column + """','yyyy-MM-dd HH').alias('col_time_agg'))"""

    source = """#time counts: compute and save results

pkl_fname = './pkl/""" + table + """.""" + column + """.TimeCounts.pkl'

if not os.path.isfile(pkl_fname):
    """ + coldatardf + """
    
    col_data_df = col_data_rdf \\
        .groupby('col_time_agg') \\
        .count() \\
        .sort('col_time_agg', ascending=True) \\
        .toPandas()
    
    col_data_df.to_pickle(pkl_fname)
"""
    return source

def get_source_c_time_counts_results(den_nb_conf,nb_cell):
    fn_results = './pkl/' + nb_cell['table'] + '.' + nb_cell['column'] + '.TimeCounts.pkl'
    source = '''#time counts: load and show results 
print "date of results: %s" % time.ctime(os.path.getmtime("''' + fn_results + '''"))
print ""
col_data_df = pd.read_pickle("''' + fn_results + '''")

col_dt = col_data_df['col_time_agg']
col_count = col_data_df['count']

trace1 = Bar(x=col_dt, y=col_count,marker=Marker(color='#E3BA22'))
data = Data([trace1])
layout = Layout(
    title="''' + nb_cell['table'] + '.' + nb_cell['column'] + '''",
    showlegend=False,
    yaxis= YAxis(
        title='count',
        zeroline=False,
        gridcolor='white'
    ),
    paper_bgcolor='rgb(233,233,233)', 
    plot_bgcolor='rgb(233,233,233)',
)
fig = Figure(data=data, layout=layout)
plotly.offline.iplot(fig)
'''
    return source



def get_source_c_cat_counts(den_nb_conf,nb_cell):
    
    table = nb_cell['table']
    column = nb_cell['column']

    source = """#category counts: compute and save results

pkl_fname = './pkl/""" + table + """.""" + column + """.CategoryCounts.pkl'

if not os.path.isfile(pkl_fname):
    col_data_df = table_rdf \\
        .select((table_rdf.""" + column + """).alias('cat')) \\
        .groupby('cat') \\
        .count() \\
        .sort('count', ascending=False) \\
        .toPandas()
    
    col_data_df.to_pickle(pkl_fname)
"""
    return source

def get_source_c_cat_counts_results(den_nb_conf,nb_cell):
    fn_results = './pkl/' + nb_cell['table'] + '.' + nb_cell['column'] + '.CategoryCounts.pkl'
    source = '''#category counts: load and show results 
print "date of results: %s" % time.ctime(os.path.getmtime("''' + fn_results + '''"))
print ""
col_data_df = pd.read_pickle("''' + fn_results + '''")

col_cat = col_data_df['cat']
col_count = col_data_df['count']

trace1 = Bar(x=col_cat, y=col_count,marker=Marker(color='#E3BA22'))
data = Data([trace1])
layout = Layout(
    title="''' + nb_cell['table'] + '.' + nb_cell['column'] + '''",
    showlegend=False,
    xaxis= XAxis(
        type='category'
    ),
    yaxis= YAxis(
        title='count',
        zeroline=False,
        gridcolor='white'
    ),
    paper_bgcolor='rgb(233,233,233)', 
    plot_bgcolor='rgb(233,233,233)',
)
fig = Figure(data=data, layout=layout)
plotly.offline.iplot(fig)
'''
    return source


def get_source_c_cat_many_counts_results(den_nb_conf,nb_cell):
    fn_results = './pkl/' + nb_cell['table'] + '.' + nb_cell['column'] + '.CategoryCounts.pkl'
    source = '''#category counts: load and show results 
print "date of results: %s" % time.ctime(os.path.getmtime("''' + fn_results + '''"))
print ""
col_data_df = pd.read_pickle("''' + fn_results + '''")
print
print col_data_df.iloc[0:10]
'''
    return source

def get_source_c_cat_freq_counts(den_nb_conf,nb_cell):
    
    table = nb_cell['table']
    column = nb_cell['column']
    
    source = """#category frequency counts: compute and save results

pkl_fname = './pkl/""" + table + """.""" + column + """.CategoryFreqCounts.pkl'

if not os.path.isfile(pkl_fname):
    col_data_df = table_rdf \\
        .select(psf.col('""" + column + """').alias('cat')) \\
        .groupby('cat') \\
        .count() \\
        .select(psf.col('count').alias('cnt')) \\
        .groupby('cnt') \\
        .count() \\
        .select(psf.col('cnt'), psf.col('count').alias('freq')) \\
        .sort('cnt', ascending=True) \\
        .toPandas()
    
    col_data_df.to_pickle(pkl_fname)
"""
    return source

def get_source_c_cat_freq_counts_results(den_nb_conf,nb_cell):
    fn_results = './pkl/' + nb_cell['table'] + '.' + nb_cell['column'] + '.CategoryFreqCounts.pkl'
    source = '''#category frequency counts: load and show results 
print "date of results: %s" % time.ctime(os.path.getmtime("''' + fn_results + '''"))
print ""
col_data_df = pd.read_pickle("''' + fn_results + '''")

col_cnt = col_data_df['cnt']
col_freq = col_data_df['freq']

trace1 = Bar(x=col_cnt, y=col_freq,marker=Marker(color='#E3BA22'))
data = Data([trace1])
layout = Layout(
    title="''' + nb_cell['table'] + '.' + nb_cell['column'] + '''",
    showlegend=False,
    yaxis= YAxis(
        title='frequency',
        zeroline=False,
        gridcolor='white'
    ),
    xaxis= XAxis(
        title='count'),
    paper_bgcolor='rgb(233,233,233)', 
    plot_bgcolor='rgb(233,233,233)',
)
fig = Figure(data=data, layout=layout)
plotly.offline.iplot(fig)
'''
    return source