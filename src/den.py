import den_notebook
import den_blocks_spark as den_blocks
import json
import os, re
import argparse

def runtype_newconf(parsed_args):
    print 'creating new DEN notebook configuration file...'
    
    den_nb = den_notebook.Den('hcat', nb_name=parsed_args.notebook, hcat_table=parsed_args.table)
    den_nb.save_conf()
    
    print ' --> ' + den_nb.name + '.den_nb_conf.json created'
    return

def runtype_conf2nb(parsed_args):
    print 'creating new DEN notebook from ' + parsed_args.notebook + '.den_nb_conf.json'
    
    den_nb = den_notebook.Den('conf', nb_name=parsed_args.notebook)   
    den_nb.conf2nb()
    den_nb.save_nb()
    
    print ' --> ' + den_nb.name  + '.ipynb created'
    return

def runtype_execute(parsed_args):
    print 'executing ' + parsed_args.notebook + '.ipynb...'
    
    den_nb = den_notebook.Den('nb', nb_name=parsed_args.notebook)
    den_nb.execute()
    den_nb.save_nb()
    
    print ' --> ' + den_nb.name + '.ipynb executed.'
    return

def runtype_autocol(parsed_args):
    print 'auto assigning column types for ' + parsed_args.notebook + '.ipynb...'
    
    den_nb = den_notebook.Den('conf', nb_name=parsed_args.notebook)
    den_nb.autocol()
    den_nb.save_conf()
    
    #den_notebook.den_nb_auto_assign_col_types_spark(den_nb_name)
    print ' --> ' + den_nb.name + '.ipynb, column types assigned.'
    return

def runtype_quickscan(parsed_args):
    # newconf
    print 'creating new DEN notebook...'
    den_nb = den_notebook.Den('hcat', nb_name=parsed_args.notebook, hcat_table=parsed_args.table)
    den_nb.conf2nb()
    den_nb.execute()
    den_nb.save_conf()
    den_nb.save_nb()
    print ' --> ' + den_nb.name + '.ipynb created and executed.'
    
    return

def runtype_fullscan(parsed_args):
    # newconf
    print 'creating new DEN notebook...'
    den_nb = den_notebook.Den('hcat', nb_name=parsed_args.notebook, hcat_table=parsed_args.table)
    den_nb.conf2nb()
    den_nb.execute()
    den_nb.autocol()
    den_nb.conf2nb()
    den_nb.execute()
    den_nb.save_conf()
    den_nb.save_nb()
    print ' --> ' + den_nb.name + '.ipynb created and executed.'
    
    return

def runtype_getnotes(parsed_args):
    den_nbs_names = [nb for nb in os.listdir('.') if re.search('.*\.ipynb$',nb)]
        
    notes_md = ''
    for den_nb_name in den_nbs_names:
        print den_nb_name[:-6]
        den_nb = den_notebook.Den('nb', nb_name=den_nb_name[:-6])
        nb_notes = den_nb.get_notes(parsed_args.item_select)
        
        if nb_notes != '':
            notes_md = notes_md + nb_notes + '\n***\n***\n'   
        
    # save to markdown file
    fn_notes = parsed_args.filename_notes 
    if not fn_notes.endswith('.md'):
        fn_notes = fn_notes.join('.md')
    with open(fn_notes, 'w') as f:
        f.write(notes_md.encode('utf8'))

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

   # getnotes
   ap_sub_getnotes = ap_sub.add_parser('getnotes', help='collect notes from all notebooks in current directory')
   ap_sub_getnotes.set_defaults(cmd=lambda x:runtype_getnotes(x))
   ap_sub_getnotes.add_argument('-i','--item_select', help='select which items to collect', choices=['all','open','status'], default='all')
   ap_sub_getnotes.add_argument('-f','--filename_notes', help='filename without extension for notes', default='notes.md')

   ap_parsed = ap.parse_args()
   ap_parsed.cmd(ap_parsed)