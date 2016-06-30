import re
import os
import io
import json
import pickle

from nbformat.v4 import (new_code_cell, new_markdown_cell, new_notebook)
from nbformat import writes
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.preprocessors.execute import CellExecutionError

# this import enables using other block definitions, just import some other file as den_blocks. Would be nice to create defaults and command line arguments for this
import den_blocks_spark as den_blocks

import den_hadoop
import den_utils


def den_nb_save(den_nb, fname):
    filestr = writes(den_nb, version=4)
    with open(fname, 'w') as f:
        f.write(filestr.encode('utf8'))
    return

def den_nb_load(fname):
    print fname
    with io.open(fname,'r', encoding='utf-8') as f:
        den_nb = nbformat.read(f, as_version=4)
    return den_nb

def den_nb_conf_new(den_nb_name,table_hcat):
    block_sequence = 1
    # get table schema via spark; change when hcatweb API is available
    table_schema = den_hadoop.table_schema_from_spark(table_hcat)
    
    den_nb_conf = {'filename': den_nb_name + '.ipynb'
                   , 'name':'Data Exploration Notebook'
                   , 'description': 'This is the description of the notebook'
                   , 'tables':{
                               table_hcat:{'hdfs': ''
                                           , 'block_sequence':block_sequence
                                           , 'column_separator': ''
                                           , 'description': ''
                                           , 'den_type':'table'
                                           , 'columns':{}
                                           }
                               }
                   , 'cells':{}
                   }

    
    # add columns to table_conf       
    block_sequence += 1
    for col in table_schema['columns']:
        den_nb_conf['tables'][table_hcat]['columns'][col] = {'type':table_schema['columns'][col]['type']
                                                             ,'ignore':0
                                                             ,'den_type':'column'
                                                             ,'pattern':None
                                                             ,'table_name_hcat':table_hcat
                                                             ,'block_sequence':block_sequence+table_schema['columns'][col]['col_sequence']}
       
    return den_nb_conf

def den_nb_conf_add_cells(den_nb_conf):
    #add general header cells
    views = den_blocks.block_conf['blocks']['b_nb_header']['views']
    
    for view_sequence in views:
        view_type = views[view_sequence]
        cells = den_blocks.block_conf['views'][view_type]['cells']
        for cell_sequence in cells:
            cell_type = cells[cell_sequence]
            nb_sequence = 'b000_v' + str(view_sequence).zfill(3) + '_c' + str(cell_sequence).zfill(3)
            den_nb_conf['cells'][nb_sequence]={'block':'b_nb_header','view':view_type,'cell':cell_type}

    
    # add cells for tables
    for table in den_nb_conf['tables']:
        den_type = den_nb_conf['tables'][table]['den_type']
        block_sequence = den_nb_conf['tables'][table]['block_sequence']
        
        block_type = den_blocks.block_conf['mapping'][den_type]
        views = den_blocks.block_conf['blocks'][block_type]['views']
        
        for view_sequence in views:
            view_type = views[view_sequence]
            cells = den_blocks.block_conf['views'][view_type]['cells']
            for cell_sequence in cells:
                cell_type = cells[cell_sequence]
                nb_sequence = 'b' + str(block_sequence).zfill(3) + '_v' + str(view_sequence).zfill(3) + '_c' + str(cell_sequence).zfill(3)
                den_nb_conf['cells'][nb_sequence]={'table':table,'block':block_type,'view':view_type,'cell':cell_type}
        
        # add cells for columns of table
        for column in den_nb_conf['tables'][table]['columns']:
            den_type = den_nb_conf['tables'][table]['columns'][column]['den_type']
            block_sequence = den_nb_conf['tables'][table]['columns'][column]['block_sequence']
            
            block_type = den_blocks.block_conf['mapping'][den_type]
            views = den_blocks.block_conf['blocks'][block_type]['views']
            
            for view_sequence in views:
                view_type = views[view_sequence]
                cells = den_blocks.block_conf['views'][view_type]['cells']
                for cell_sequence in cells:
                    cell_type = cells[cell_sequence]
                    nb_sequence = 'b' + str(block_sequence).zfill(3) + '_v' + str(view_sequence).zfill(3) + '_c' + str(cell_sequence).zfill(3)
                    den_nb_conf['cells'][nb_sequence]={'table':table,'column':column,'block':block_type,'view':view_type,'cell':cell_type}
     
    #add general footer cells
    views = den_blocks.block_conf['blocks']['b_nb_footer']['views']
    
    for view_sequence in views:
        view_type = views[view_sequence]
        cells = den_blocks.block_conf['views'][view_type]['cells']
        for cell_sequence in cells:
            cell_type = cells[cell_sequence]
            nb_sequence = 'b999_v' + str(view_sequence).zfill(3) + '_c' + str(cell_sequence).zfill(3)
            den_nb_conf['cells'][nb_sequence]={'block':'b_nb_footer','view':view_type,'cell':cell_type}
                    
    return den_nb_conf

def den_nb_auto_assign_col_types_spark(den_nb_name):
    # load notebook configuration
    den_nb_conf_file = den_nb_name + '.den_nb_conf.json'
    with open(den_nb_conf_file, 'r') as f:
        den_nb_conf = json.load(f)
    
    for table in den_nb_conf['tables']:
        for column in den_nb_conf['tables'][table]['columns']:
            try:
                # load basic statistics
                col_stats = {}
                with open('./pkl/' + table + '.' + column + '.ColumnCounts.pkl', 'rb') as f:
                    col_stats = pickle.load(f)
                # compute column properties
                col_all = col_stats['count']['val']
                col_distinct = col_stats['distinct']['val']
                col_empty = int(col_stats['empty']['val']) if col_stats['empty']['val'] else 0
                col_nulls = int(col_stats['nulls']['val']) if col_stats['nulls']['val'] else 0
                col_vals = col_all - col_empty - col_nulls
                col_distinct_rel = col_distinct / (col_vals * 1.0)
                col_min = col_stats['min']['val']
                col_max = col_stats['max']['val']
                
                #determine column types
                pattern = None
                if col_distinct == 1:
                    den_type = 'singlevalue'
                # datetimes
                elif den_utils.is_date(col_max):
                    pattern = den_utils.get_date_pattern(col_max)
                    den_type = 'time'
                # timestames
                elif den_utils.is_ts(col_min,col_max):
                    pattern = 'timestamp'
                    den_type = 'time'
                # categorical values
                elif col_distinct_rel <= 0.9:
                    if col_distinct_rel <= 0.2 and col_distinct <= 1000:
                        den_type = 'category'
                    else:
                        den_type = 'category_many'
                else:
                    den_type = 'column'
                
                den_nb_conf['tables'][table]['columns'][column]['den_type'] = den_type
                den_nb_conf['tables'][table]['columns'][column]['pattern'] = pattern
                
                # print assigned column type
                if pattern:
                    pattern_log = ' (pattern: ' + pattern + ')'
                else:
                    pattern_log = ''
                print column + '\n -> column type "' + den_type + '" assigned' + pattern_log
            
            except:
                den_nb_conf['tables'][table]['columns'][column]['den_type'] = 'column'
                den_nb_conf['tables'][table]['columns'][column]['pattern'] = None
                print column + '\n -> auto assign not possible, default column type assigned'
    
    with open(den_nb_conf_file, 'wb') as f:
        json.dump(den_nb_conf, f, indent=4)
        
    print den_nb_name
    return


def den_nb_generate(den_nb_conf):
    nb_cells = []
    for cell in sorted(den_nb_conf['cells'].keys()):
        den_cell_type = den_nb_conf['cells'][cell]['cell']
        nb_cell_type = den_blocks.block_conf['cells'][den_cell_type]['nb_cell_type']
        cell_source = den_blocks.block_conf['cells'][den_cell_type]['source']
        if hasattr(den_blocks,cell_source):
            source = getattr(den_blocks,cell_source)(den_nb_conf,den_nb_conf['cells'][cell])
        else:
            source = cell_source
        
        if (nb_cell_type == 'markdown'):
            nb_cells.append(new_markdown_cell(source=source))
        elif (nb_cell_type == 'code'):
            nb_cells.append(new_code_cell(source=source,metadata={'code_folding':[0]}))
            
    den_nb = new_notebook(cells=nb_cells)
     
    return den_nb

def den_nb_execute(den_nb_name):
    nb = den_nb_load(den_nb_name+'.ipynb')
    
    ep = ExecutePreprocessor(timeout=1800, kernel_name='python2',allow_errors=True)
    #try:
    ep.preprocess(nb, {'metadata': {'path': '.'}})
    #except CellExecutionError:
    #    print "Error during cell execution, see notebook for error message..."
    #    raise
    #finally:
    den_nb_save(nb,den_nb_name + '.ipynb')

    return

def den_nb_add_block(den_nb_name,table_col_name,den_col_type):
    if table_col_name == '':
        table_col_name = 'new column'
    if den_col_type == '':
        den_col_type = 'category'
    # create minimum config for new notebook with the block to be added
    den_nb_conf = {'tables':{
                               'database.table':{
                                           'columns': {
                                                        table_col_name: { 
                                                            'den_type': den_col_type, 
                                                            'block_sequence': 1,
                                                            'pattern': None
                                                        }
                                           }
                               }
                   }
                   ,'cells':{}
                   }
    table = 'database.table'
    
    # add cells to config based on den_col_type
    # FIXME lelijk: is nu een kopie van code in den_nb_conf_add_cells()
    for column in den_nb_conf['tables'][table]['columns']:
        den_type = den_nb_conf['tables'][table]['columns'][column]['den_type']
        block_sequence = den_nb_conf['tables'][table]['columns'][column]['block_sequence']
        
        block_type = den_blocks.block_conf['mapping'][den_type]
        views = den_blocks.block_conf['blocks'][block_type]['views']
        
        for view_sequence in views:
            view_type = views[view_sequence]
            cells = den_blocks.block_conf['views'][view_type]['cells']
            for cell_sequence in cells:
                cell_type = cells[cell_sequence]
                nb_sequence = 'b' + str(block_sequence).zfill(3) + '_v' + str(view_sequence).zfill(3) + '_c' + str(cell_sequence).zfill(3)
                den_nb_conf['cells'][nb_sequence]={'table':table,'column':column,'block':block_type,'view':view_type,'cell':cell_type}    
    # FIXME end
    
    # generate den_nb containing just one block
    den_nb = den_nb_generate(den_nb_conf)
    # load existing den_nb to which block will be added
    src_den_nb = den_nb_load(den_nb_name + '.ipynb')
    # add cells to existing den_nb 
    src_den_nb.cells.extend(den_nb.cells)
    # save den_nb including the added block
    den_nb_save(src_den_nb, den_nb_name + '.ipynb')
    
    #print src_den_nb  

    return





def clean_notes(notes_md):
    # remove headers with no notes (6 times, for cleaning up to 6 levels of markdown headers)
    new_notes_md = notes_md
    for h in range(1,6):
        lines_notes_md = new_notes_md.split('\n')
        new_notes_md = ''
        i = 0
        while i < len(lines_notes_md):
            # if header found, search forward for next header
            if lines_notes_md[i] and lines_notes_md[i][0] == "#":
                j = i
                rel_notes_md = ''
                while j + 1 < len(lines_notes_md):
                    j += 1
                    if lines_notes_md[j]:
                        if re.match('^([-+\*\s]|status)\s.*',lines_notes_md[j],re.IGNORECASE):
                            rel_notes_md = rel_notes_md + lines_notes_md[j] + '\n'
                        if lines_notes_md[j][0] == "#":
                            break
                # add relevant lines if found any
                if rel_notes_md != '':
                    new_notes_md = new_notes_md + lines_notes_md[i] + '\n' + rel_notes_md +'\n'
                # if no relevant lines are found only add next header if it is smaller than previous header
                elif lines_notes_md[j].count('#',0,5) > lines_notes_md[i].count('#',0,5):
                    new_notes_md = new_notes_md + lines_notes_md[i] + '\n'
                i = j - 1 
            i += 1
    return new_notes_md

def den_nb_get_notes_nb(den_nb_name,notes_items):
    with open(den_nb_name) as f:
        nb = nbformat.read(f, as_version=4)
    
    if notes_items == 'all':
        re_rel_lines = '^\s*[-+#\*].*'
        re_irrel_lines = '^#{4}.*'
    elif notes_items == 'open':
        re_rel_lines = '^\s*[-#].*'
        re_irrel_lines = '^#{4}.*'
    elif notes_items == 'status':
        re_rel_lines = '^\s*(#|status).*'
        re_irrel_lines = '^#{4}.*'
     
    # get relevant lines  
    lines = ''
    for nb_src_cell in nb['cells']:
        if (nb_src_cell['cell_type'] == 'markdown'):
            for line in nb_src_cell['source'].split('\n'):
                if line and re.match(re_rel_lines,line,re.IGNORECASE) and not re.match(re_irrel_lines,line,re.IGNORECASE):
                    lines = lines + line + '\n'
    
    # add empty line before each header
    notes_md = ''
    for line in lines.split('\n'):
        if notes_md and line and line[0] == '#':
            notes_md = notes_md + '\n'
        notes_md = notes_md + line + '\n' 
    
    # remove headers with no notes 
    notes_md = clean_notes(notes_md)
    
    # escape markdown special characters
    notes_md = notes_md.replace('_','\_')
            
    return notes_md

def den_nb_get_notes_nbs(notes_items,fn_notes):
    den_nbs_names = [nb for nb in os.listdir('.') if re.search('.*\.ipynb$',nb)]
        
    notes_md = ''
    for den_nb_name in den_nbs_names:
        nb_notes = den_nb_get_notes_nb(den_nb_name,notes_items)
        if nb_notes != '':
            notes_md = notes_md + nb_notes + '\n***\n***\n'   
        
    # save to markdown file
    with open(fn_notes + '.md', 'w') as f:
        f.write(notes_md.encode('utf8'))
    
    return

