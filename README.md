# den
## The data exploration notebook
* Have all code, results, visualisations and notes for data exploration in one Jupyter Notebook
* Generate this notebook automatically from a HCatalog table name. 
* Collect notes from several data exploration notebooks into one markdown document
* Currently configured to use Spark, however changing the configuration to work with e.g. Hive or Pig and/or any mix of tools (as long as it can be run from Jupyter) is easy.

## High level approach for generation of the data exploration notebook
* Get metadata from HCatalog
* Generate initial notebook
* Compute basic statistics for each column
* Determine column types from data
  * column types determine the cells that will be added to the notebook:
    * column types map to block types (e.g. the datetime block)
    * blocks contain views (e.g. the view to show the number of records in time)
    * views contain cells (e.g. a cell to compute results with Spark and a cell to visualise the results within the notebook with Plotly)
    * cells contain code or markdown (e.g. notes in markdown, related to the datetime view) 
  * current column types are:
    * datetime (various formats including unix timestamps)
    * categorical data (with different views depending on number of categories)
    * single value columns (to identify less interesting columns)
    * general column type (the default column type)
* Perform column type specific analyses
* Create data exploration notebook with column type specific statistics and visualisations
