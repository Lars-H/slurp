# CROP - Setup and Usage Guide

## Installation

In the directory, a virtual environment (`venv` directory) is provided to execute the engine.
*Alternatively*, you can install the requirements provided in the `requirements.txt` file to a local Python using pip:
```bash
pip2 install -r requirements.txt
```

It is advised to use a virtual environment for the installation.
Once the requirements are installed, the engine can be executed using the command line.

## Usage

Example: 
```
venv/bin/python crop.py -f example.rq -s http://fragments.dbpedia.org/2014/en 
```
(Note that the engine currently only supports conjunctive SPARQL queries.)


## Help

You can find the help using:
```bash
venv/bin/python crop.py -h
```
and get the usage options:
```bash
usage: crop.py [-h] [-s SERVER] [-f QUERYFILE] [-q QUERY] [-r {y,n,f,all}]
               [-e EDDIES] [-p {NoPolicy,Ticket,Random}] [-t TIMEOUT]
               [-v {INFO}] [-d HEIGHT_DISCOUNT] [-c COST_THRESHOLD]
               [-u ROBUST_THRESHOLD] [-k K] [-a {True,False}] [-b TOP_T]

CROP: An nLDE-based TPF Client with a cost model-based robust query plan
optimizer

optional arguments:
  -h, --help            show this help message and exit
  -s SERVER, --server SERVER
                        URL of the triple pattern fragment server (required)
  -f QUERYFILE, --queryfile QUERYFILE
                        file name of the SPARQL query (required, or -q)
  -q QUERY, --query QUERY
                        SPARQL query (required, or -f)
  -r {y,n,f,all}, --printres {y,n,f,all}
                        format of the output results
  -e EDDIES, --eddies EDDIES
                        number of eddy processes to create
  -p {NoPolicy,Ticket,Random}, --policy {NoPolicy,Ticket,Random}
                        routing policy used by eddy operators
  -t TIMEOUT, --timeout TIMEOUT
                        query execution timeout
  -v {INFO}, --verbose {INFO}
                        print logging information
  -d HEIGHT_DISCOUNT, --height_discount HEIGHT_DISCOUNT
                        Discount for NLJ higher in the plan (Optional)
  -c COST_THRESHOLD, --cost_threshold COST_THRESHOLD
                        Cost threshold for the query plan optimizer(Optional)
  -u ROBUST_THRESHOLD, --robust_threshold ROBUST_THRESHOLD
                        Robustness threshold for the query plan
                        optimizer(Optional)
  -k K, --k K           Parameter k in IDP (Optional)
  -a {True,False}, --adaptive_k {True,False}
                        Adaptive k (Optional)
  -b TOP_T, --top_t TOP_T
                        Top t plans to consider in IDP
                        (Optional)
```

