import den_notebook
import pprint
import json
import sys, getopt


def runtype_newconf(den_nb_name,table_hcat_name):
    print 'creating new DEN notebook configuration file...'
    # create new den_nb configuration
    den_nb_conf = den_notebook.den_nb_conf_new(den_nb_name,table_hcat_name)

    den_nb_conf_file = den_nb_name + '.den_nb_conf.json'
    with open(den_nb_conf_file, 'wb') as f:
        json.dump(den_nb_conf, f, indent=4)
    print ' --> ' + den_nb_conf_file + ' created'
    return

def runtype_conf2nb(den_nb_name):
    den_nb_conf_file = den_nb_name + '.den_nb_conf.json'
    print 'creating new DEN notebook from ' + den_nb_conf_file
    with open(den_nb_conf_file, 'r') as f:
        den_nb_conf = json.load(f)
    
    # add cells based on tables and columns config
    den_nb_conf = den_notebook.den_nb_conf_add_cells(den_nb_conf)
    
    # generate notebook from den_nb configuration
    den_nb = den_notebook.den_nb_generate(den_nb_conf)
    
    # save den_nb to disk
    den_nb_fn = den_nb_conf['filename']
    den_notebook.den_nb_save(den_nb,den_nb_fn)
    
    print ' --> ' + den_nb_fn + ' created'
    return

def runtype_execute(den_nb_name):
    print 'executing ' + den_nb_name + '.ipynb...'
    den_notebook.den_nb_execute(den_nb_name)
    print ' --> ' + den_nb_name + '.ipynb executed.'
    return

def runtype_autocol(den_nb_name):
    print 'auto assigning column types for ' + den_nb_name + '.ipynb...'
    #den_notebook.den_nb_auto_assign_col_types_pig(den_nb_name)
    den_notebook.den_nb_auto_assign_col_types_spark(den_nb_name)
    print ' --> ' + den_nb_name + '.ipynb, column types assigned.'
    return

def runtype_quickscan(den_nb_name,table_hcat_name):
    runtype_newconf(den_nb_name,table_hcat_name)
    runtype_conf2nb(den_nb_name)
    runtype_execute(den_nb_name)
    return

def runtype_getnotes(notes_items,fn_notes):
    den_notebook.den_nb_get_notes_nbs(notes_items,fn_notes)
    return

def runtype_addblock(den_nb_name,table_col_name,den_col_type):
    den_notebook.den_nb_add_block(den_nb_name,table_col_name,den_col_type)
    return



def print_help():
    print 'Data Exploration Notebook'
    print ''
    print 'runtypes:'
    print ' - quickscan : create new DEN notebook with default configuration and execute all cells'
    print '        python den.py -r quickscan -n <notebook name> -t <database.table (as in hcatalog)>'
    print ' - autocol : automatically assign coltype based on basic column counts'
    print '        python den.py -r autocol -n <notebook name>'
    print ''
    print ' - newconf : create new DEN notebook configuration file'
    print '        python den.py -r newconf -n <notebook name> -t <database.table (as in hcatalog)>'
    print ' - conf2nb : create new DEN notebook from existing configuration file'
    print '        python den.py -r conf2nb -n <notebook name> '
    print ''
    print ' - execute : execute all cells in DEN notebook'
    print '        python den.py -r execute -n <notebook name>'
    print ''
    print ' - addblock: add block to the end of an existing DEN notebook'
    print '        python den.py -r execute -n <notebook name> -b <DEN column type for new block> -c <name of column>'
    print ''
    print ' - getnotes : collect all notes from notebooks in current directory'
    print '        python den.py -r getnotes -i <all,open,status> -f <filename without extension for notes>' 
    return



def main(argv):
    table_hcat_name = ''
    den_nb_name = ''
    den_nbs_names = ''
    runtype = ''
    notes_items = 'all'
    fn_notes = 'den_nb_notes'
    den_col_type = ''
    table_col_name = ''
    try:
        opts, args = getopt.getopt(argv,"ht:n:r:i:f:c:b:")
    except getopt.GetoptError:
        print_help()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_help()
            sys.exit()
        elif opt in ("-t"):
            table_hcat_name = arg
        elif opt in ("-n"):
            den_nbs_names = arg
        elif opt in ("-r"):
            runtype = arg
        elif opt in ("-i"):
            notes_items = arg
        elif opt in ("-f"):
            fn_notes = arg
        elif opt in ("-c"):
            table_col_name = arg
        elif opt in ("-b"):
            den_col_type = arg
        
    den_nb_name = den_nbs_names.split(',')[0]
    
    if (runtype == 'newconf'):
        runtype_newconf(den_nb_name,table_hcat_name)
    elif (runtype == 'conf2nb'):
        runtype_conf2nb(den_nb_name)
    elif (runtype == 'execute'):
        runtype_execute(den_nb_name)
    elif (runtype == 'autocol'):
        runtype_autocol(den_nb_name)
    elif (runtype == 'quickscan'):
        runtype_quickscan(den_nb_name,table_hcat_name)
    elif (runtype == 'getnotes'):
        runtype_getnotes(notes_items,fn_notes)
    elif (runtype == 'addblock'):
        runtype_addblock(den_nb_name,table_col_name,den_col_type)
    else:
        print "unknown runtype"
        print "run 'python den.py -h' for help"
        sys.exit(2)

if __name__ == "__main__":
   main(sys.argv[1:])
