import den_notebook
import den_blocks_spark as den_blocks
import json
import sys, getopt
import argparse

def runtype_newconf(parsed_args):
    print 'creating new DEN notebook configuration file...'
    
    den_nb_name = parsed_args.notebook
    table_hcat_name = parsed_args.table
    
    # create new den_nb configuration
    den_nb_conf = den_notebook.den_nb_conf_new(den_nb_name,table_hcat_name)

    den_nb_conf_file = den_nb_name + '.den_nb_conf.json'
    with open(den_nb_conf_file, 'wb') as f:
        json.dump(den_nb_conf, f, indent=4)
    print ' --> ' + den_nb_conf_file + ' created'
    return

def runtype_conf2nb(parsed_args):
    den_nb_name = parsed_args.notebook
    
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

def runtype_execute(parsed_args):
    den_nb_name = parsed_args.notebook

    print 'executing ' + den_nb_name + '.ipynb...'
        
    den_notebook.den_nb_execute(den_nb_name)
    print ' --> ' + den_nb_name + '.ipynb executed.'
    return

def runtype_autocol(parsed_args):
    den_nb_name = parsed_args.notebook
    
    print 'auto assigning column types for ' + den_nb_name + '.ipynb...'
    
    #den_notebook.den_nb_auto_assign_col_types_pig(den_nb_name)
    den_notebook.den_nb_auto_assign_col_types_spark(den_nb_name)
    print ' --> ' + den_nb_name + '.ipynb, column types assigned.'
    return

def runtype_quickscan(parsed_args):
    runtype_newconf(parsed_args)
    runtype_conf2nb(parsed_args)
    runtype_execute(parsed_args)
    return

def runtype_fullscan(parsed_args):
    #quickscan
    runtype_quickscan(parsed_args)
    
    #assign coltypes, convert and execute
    runtype_autocol(parsed_args)
    runtype_conf2nb(parsed_args)
    runtype_execute(parsed_args)
    return

def runtype_getnotes(parsed_args):
    item_select = parsed_args.item_select
    filename_notes = parsed_args.filename_notes
    
    den_notebook.den_nb_get_notes_nbs(item_select,filename_notes)
    return

def runtype_addblock(parsed_args):
    den_nb_name = parsed_args.notebook
    table_col_name = parsed_args.column_name
    den_col_type = parsed_args.column_type
    
    den_notebook.den_nb_add_block(den_nb_name,table_col_name,den_col_type)
    return

if __name__ == "__main__":
  
   ap = argparse.ArgumentParser()
   ap_sub = ap.add_subparsers(title='subcommands',description="'den.py {subcommand} -h' for additional help")
   
   # newconf
   ap_sub_newconf = ap_sub.add_parser('newconf', help='create new DEN notebook configuration file')
   ap_sub_newconf.set_defaults(cmd=lambda x:runtype_newconf(x))
   ap_sub_newconf.add_argument('-t','--table', help='database.table (as in hcatalog)', required=True)
   ap_sub_newconf.add_argument('-n','--notebook', help='notebook name', required=True)
   
   # conf2nb
   ap_sub_conf2nb = ap_sub.add_parser('conf2nb', help='create new DEN notebook from existing configuration file')
   ap_sub_conf2nb.set_defaults(cmd=lambda x:runtype_conf2nb(x))
   ap_sub_conf2nb.add_argument('-n','--notebook', help='notebook name',required=True)
   
   # execute
   ap_sub_execute = ap_sub.add_parser('execute', help='execute all cells in DEN notebook')
   ap_sub_execute.set_defaults(cmd=lambda x:runtype_execute(x))
   ap_sub_execute.add_argument('-n','--notebook', help='notebook name',required=True)
   
   # autocol
   ap_sub_autocol = ap_sub.add_parser('autocol', help='automatically assign coltype based on basic column counts')
   ap_sub_autocol.set_defaults(cmd=lambda x:runtype_autocol(x))
   ap_sub_autocol.add_argument('-n','--notebook', help='notebook name',required=True)
   
   # quickscan
   ap_sub_qscan = ap_sub.add_parser('quickscan', help='create new DEN notebook with default column types and execute all cells')
   ap_sub_qscan.set_defaults(cmd=lambda x:runtype_quickscan(x))
   ap_sub_qscan.add_argument('-t','--table', help='database.table (as in hcatalog)', required=True)
   ap_sub_qscan.add_argument('-n','--notebook', help='notebook name', required=True)
   
   # fullscan
   ap_sub_fscan = ap_sub.add_parser('fullscan', help='create new DEN notebook with automatically assigned column types and execute all cells')
   ap_sub_fscan.set_defaults(cmd=lambda x:runtype_fullscan(x))
   ap_sub_fscan.add_argument('-t','--table', help='database.table (as in hcatalog)', required=True)
   ap_sub_fscan.add_argument('-n','--notebook', help='notebook name', required=True)

   # addblock
   column_types = den_blocks.block_conf['mapping'].keys()
   
   ap_sub_addblock = ap_sub.add_parser('addblock', help='add block to the end of an existing DEN notebook')
   ap_sub_addblock.set_defaults(cmd=lambda x:runtype_addblock(x))
   ap_sub_addblock.add_argument('-n','--notebook', help='notebook name', required=True)
   ap_sub_addblock.add_argument('-c','--column_name', help='name of column', required=True)
   ap_sub_addblock.add_argument('-b','--column_type', help='DEN column type for new block', choices=column_types, required=True)
   
   # getnotes
   ap_sub_getnotes = ap_sub.add_parser('getnotes', help='collect notes from all notebooks in current directory')
   ap_sub_getnotes.set_defaults(cmd=lambda x:runtype_getnotes(x))
   ap_sub_getnotes.add_argument('-i','--item_select', help='select which items to collect', choices=['all','open','status'], required=True)
   ap_sub_getnotes.add_argument('-f','--filename_notes', help='filename without extension for notes', required=True)

   ap_parsed = ap.parse_args()
   ap_parsed.cmd(ap_parsed)