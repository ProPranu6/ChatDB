import json as J
import os
from pymongo import MongoClient
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import CFG
from nltk.tree import Tree
nltk.download('stopwords')
from fuzzywuzzy import process
from datamuse import Datamuse
from warnings import filterwarnings
import sqlite3
import json
import os
import re

filterwarnings('ignore')


KEYWORDS = ['by', 'where', 'order', 'group']
STOPWORDS = [ wd for wd in stopwords.words('english') if wd not in KEYWORDS]
LEMMATIZER = WordNetLemmatizer()
NUMBER_STORE = []
STRING_STORE = []




client = MongoClient()

rules = """
This package provides tools for converting natural language queries into SQL and NoSQL database queries 
for retrieving results. It operates without the use of learnable parameters, neural networks, or embeddings, 
focusing instead on rule-based natural language parsing.

Features:
1. Supports natural language descriptions of queries that involve combinations of:
   - `SELECT` statements
   - Table names
   - Column names
   - `WHERE`, `GROUP BY`, and `ORDER BY` clauses
   Column names can include aggregated operations such as `min`, `max`, `median`, `mean`, `stddev`, `variance`, 
   `count`, and `distinct`.

2. Enforces rules for SQL queries:
   - When `WHERE`, `GROUP BY`, or `ORDER BY` clauses are used, the keywords **must appear** explicitly 
     in their respective clauses.
   - Multi-word column names are enclosed in backticks (e.g., `` `column name` ``).
   - Aggregation parameters with multi-word descriptions are supported using formats like `minimum-of`, 
     `standard-deviation`, etc.

3. Supports natural language queries with filler words (e.g., prepositions, stop words) between clauses or commands 
   while ensuring syntactic correctness.

4. Ensures that table names appear only once in a query, whereas column names and aggregation operations 
   may occur multiple times.

This package is ideal for scenarios requiring accurate, rule-based query generation without relying on 
machine learning or embeddings.
"""
import io
print(rules)
def data_explore_display(file_path='NoSQL/formula_1', print_schema=True):
    if file_path.split("/")[0].lower() == 'sql':
        for sqlitef in os.listdir(file_path):
            if sqlitef.endswith('.sqlite'):
                _sqlite_tables_to_json(os.path.join(file_path, sqlitef))

    db = _load_nosql(file_path)
    schema_description = _describe_database_schema(db, dbtype=file_path.split("/")[0], print_db=print_schema)
    if print_schema:
        _schema_print_display(schema_description)
    return _schema_print_display(schema_description)
def _schema_print_display(schema_description):
    # Capture the printed output
    output = io.StringIO()
    # Print the schema
    for collection, schema in schema_description.items():
        output.write(f"Collection: {collection}\n")
        for field, data_list in schema.items():
            count, typee, zone, uniqueness, nullity, minval, maxval, distinct = data_list
            output.write(f"  Field: {field}\n")
            output.write(f"    Data-Type: {typee}, Count: {count}, Concpt-Type: {zone}, Unique: {True if uniqueness == 'unique' else False}, Null: {True if nullity == 'null' else False} " + 
                         (f"Min: {minval}, Max: {maxval}" if zone == 'num' else "") + 
                         (f"Distinct : {distinct}" if zone == 'str' else "") + "\n")
    # Return the captured output
    return output.getvalue()
def _load_nosql(database_dir):
    dbname = os.path.basename(database_dir.rstrip("/"))
    db = client[dbname]
    
    for path in os.listdir(database_dir):
        if path.endswith('.json'):
            collection_name = path.split(".")[0]
            if collection_name not in db.list_collection_names():
                collection = db[collection_name]

                collection_objs = J.load(open(os.path.join(database_dir, path)))
                collection.insert_many(collection_objs) 
    return db




def _sqlite_tables_to_json(sqlite_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()
    
    # Get the list of all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # Ensure the output folder exists
    os.makedirs(os.path.dirname(sqlite_file), exist_ok=True)
    
    for table_name in tables:
        table_name = table_name[0]  # Extract table name from tuple
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Get column names
        column_names = [description[0] for description in cursor.description]
        
        # Convert rows to a list of dictionaries
        table_data = [dict(zip(column_names, row)) for row in rows]
        
        # Write table data to a separate JSON file
        output_file = os.path.join(os.path.dirname(sqlite_file), f"{table_name}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(table_data, f, indent=4)
        
        #print(f"Table '{table_name}' has been written to {output_file}")
    
    # Close the connection
    conn.close()



def _documents_print(cursor):
    cursor_list = list(cursor)
    for d in cursor_list:
        for k, v in d.items():
            print(f"{k} : {v}", end=" | ")
        print("\n" + ("*" + "-"*25 + "*").center(125, " "))

def _classify_integer_field(values):
    """
    Classify whether an integer field is ordinal/nominal or continuous.
    """
    unique_values = set(values)
    if (len(unique_values) == len(values)) or (len(unique_values) <= 0.25*len(values)):  # Threshold can be adjusted
        return f"{_ordinal_nominal(values)}-cat"
    else:
        return "num"

def _uniqueness(values):
    unique_values = set(values)
    if len(unique_values) == len(values):
        return "unique"
    else:
        return "non-unique"

def _ordinal_nominal(values):
    if len(values) == 1:
        return 'ordinal'
    
    vsorted =  sorted(values)   
    vmin, vmax, d, n = vsorted[0], vsorted[-1], vsorted[1]-vsorted[0], len(values)-1

    return ('ordinal' if (n*d == vmax-vmin) else 'nominal')


from collections import defaultdict 
def _get_schema(db, collection_name, db_type='NoSQL'):

    if os.path.exists(f'./{db_type}/{db.name}/schemas/{collection_name}.json'):
        return dict(J.load(open(f'./{db_type}/{db.name}/schemas/{collection_name}.json')))
    else:
        os.makedirs(f'./{db_type}/{db.name}/schemas', exist_ok=True)
    
    schema = defaultdict(lambda: [0, '', '', 'unique', 'non-null', -1, -1, []])  #count data-type, conpt-type, uniqueness, nullity, minvalue, maxvalue, unique_values
    collection = db[collection_name]
    
    int_values = defaultdict(list)
    key_values = defaultdict(list)
   
    # Sample documents to infer schema
    sample_docs = collection.find()
    
    for doc in sample_docs:
        for key, value in doc.items():
            field_type = type(value).__name__
            schema[key][0] += 1
            if schema[key][1] in ['', 'NoneType']:
                schema[key][1] = field_type
            
            if field_type == 'NoneType':
                schema[key][4] = 'null'
            
            if field_type == 'int':
                int_values[key].append(value)
            
            key_values[key].append(value)
    
    for key, values in int_values.items():
        classification = _classify_integer_field(values)
        schema[key][2] = classification # = schema[key].pop('int')
        
    
    for key, values in key_values.items():
        uniqueness_value = _uniqueness(values)
        
        schema[key][3] = uniqueness_value  # = schema[key].pop('int')

        if schema[key][1] in ['str', 'dict', 'ObjectId']:
                schema[key][2] = 'cat'
                if schema[key][1] == 'str':
                    schema[key][7] = list(set(values))    
        elif schema[key][1] in ['datetime', 'float']:
                schema[key][2] = 'num'
                schema[key][5], schema[key][6] = min(filter(lambda x: x!=None, values)), max(filter(lambda x: x!=None, values))
        elif schema[key][1] in ['NoneType']:
                schema[key][2] = 'null'
        else:
            schema[key][5], schema[key][6] = min(filter(lambda x: x!=None, values)), max(filter(lambda x: x!=None, values))
            pass 

    
    J.dump(schema, open(f'./{db_type}/{db.name}/schemas/{collection_name}.json', 'w'))
    return schema

def _describe_database_schema(db, dbtype='NoSQL', print_db=False):
    database_schema = {}
    if print_db:
        print(f"Database : {db.name}")
    for collection_name in db.list_collection_names():
        #print(f"Describing schema for collection: {collection_name}")
        schema = _get_schema(db, collection_name, dbtype)
        database_schema[collection_name] = schema
    
    return database_schema

def _schema_print(schema_description):
    # Print the schema
    for collection, schema in schema_description.items():
        print(f"Collection: {collection}")
        for field, data_list in schema.items():
            print(f"  Field: {field}")
            count, typee, zone, uniqueness, nullity, minval, maxval, distinct = data_list
            print(f"    Data-Type: {typee}, Count: {count}, Concpt-Type: {zone}, Unique: {True if uniqueness == 'unique' else False}, Null: {True if nullity == 'null' else False} " + (f"Min: {minval}, Max: {maxval}" if zone=='num' else "") + (f"Distinct : {distinct}" if zone=='str' else "") )

def data_ingest(file_path='NoSQL/formula_1'):
    _load_nosql(file_path)
    return 

def data_explore(file_path='NoSQL/formula_1', print_schema=True):
    if file_path.split("/")[0].lower() == 'sql':
        for sqlitef in os.listdir(file_path):
            if sqlitef.endswith('.sqlite'):
                _sqlite_tables_to_json(os.path.join(file_path, sqlitef))

    db = _load_nosql(file_path)
    schema_description = _describe_database_schema(db, dbtype=file_path.split("/")[0], print_db=print_schema)
    if print_schema:
        _schema_print(schema_description)
    return schema_description

import random

def generate_sql(schema_description):
    order_queries = defaultdict(lambda : defaultdict(set))
    for collection in schema_description:
        query_pattern = {'cmd' : ['SELECT'],
                        'cat' : [""], 'num':[""], 'nom-cat':[""], 'ord-cat':[""], 
                        'agg': ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 'MEDIAN'], 
                        'clause':['WHERE', 'GROUP BY', 'ORDER BY'],
                        'numeric_op' : ['>', '<', '>=', '<=', '='] }
        
        for col in schema_description[collection]:
            _type = schema_description[collection][col][2]
            
            if _type  == 'cat':
                query_pattern['cat'] += [col]
            elif _type  == 'nominal-cat':
                query_pattern['nom-cat'] += [col]
            elif _type  == 'ordinal-cat':
                query_pattern['ord-cat'] += [col]
            else:
                query_pattern['num'] += [col]
        
       
        n = random.randint(10, 15)
        _ = 0

        

        while _ < n:
            query = ""
            cmd = random.choice(query_pattern['cmd'])
            query += cmd + " "

            agg = random.choice(query_pattern['agg'] + [""])
            
            if agg == "":
                _A_ = random.choice(query_pattern['cat'] + query_pattern['num']+ query_pattern['nom-cat'] + query_pattern['ord-cat'])
                if _A_ == "":
                    continue
                query += _A_ + f" FROM {collection} "
            elif agg == "COUNT":
                _A_ = random.choice(query_pattern['cat'] + query_pattern['num']+ query_pattern['nom-cat'] + query_pattern['ord-cat'] + ["*"])
                if _A_ == "":
                    continue
                query += f"{agg}({_A_}) FROM {collection} "
            elif agg == "MIN":
                _A_ = random.choice(query_pattern['num']+ query_pattern['ord-cat'])
                if _A_ == "":
                    continue
                query += f"{agg}({_A_}) FROM {collection} "
            elif agg == "MAX":
                _A_ = random.choice(query_pattern['num']+ query_pattern['ord-cat'])
                if _A_ == "":
                    continue
                query += f"{agg}({_A_}) FROM {collection} "
            elif agg == "MEDIAN":
                _A_ = random.choice(query_pattern['num']+ query_pattern['ord-cat'])
                if _A_ == "":
                    continue
                query += f"{agg}({_A_}) FROM {collection} "
            else:
                num = random.choice(query_pattern['num'])
                query += f"{agg}({num}) FROM {collection} "
                if num == "":
                    continue
            
            clause = random.choice(query_pattern['clause']+[""])

            if clause == "":
                pass
            elif clause == "WHERE":
                if random.random() >0.5:
                    num = random.choice(query_pattern['num'])
                    if num == "":
                        continue
                    nop = random.choice(query_pattern['numeric_op'])
                    col = num
                    numv = random.randint(int(schema_description[collection][col][5]), int(schema_description[collection][col][6]+1))
                    query += f"WHERE {num} {nop} {numv} "
                else:
                    cat = random.choice(query_pattern['cat'])
                    col = cat
                    if cat == "":
                        continue
                    catop = random.choice(["=", "!="])
                    catv = random.choice(schema_description[collection][col][7] + [""])
                    if catv == "":
                        continue
                    query += f"WHERE {cat} {catop} '{catv}' "
            elif clause == "ORDER BY":
                num = random.choice(query_pattern['num'] + query_pattern['ord-cat'])
                if num == "":
                    continue
                query += f"ORDER BY {num}"
            elif clause == "GROUP BY":
                cat = random.choice(query_pattern['cat'] + query_pattern['nom-cat'])
                if cat == "":
                    continue
                query += f"GROUP BY {cat}"

            
            order_queries[agg][clause].add(query)
               
        
            _ += 1
    
    order_queries = dict(order_queries)
    
    gens = []
    for agg, clauses in order_queries.items():
        clauses = dict(clauses)
        for clause, queries in clauses.items():
            print(f"{agg} | {clause}", end="\n\t")
            for query in queries:
                print(query, end="\n\t")
                gens.append(query)

            print("")
            print("-"*100)
    return gens



def _split_ignore_quoticks(s):
    return re.findall(r"'[^']*'|`[^`]*`|\S+", s)


def _preprocess(wd, corrs=[]):
    
    #wd = wd.lower().strip(",")

    if wd.lower().strip(",") in STOPWORDS:
        return ""
    
    
    #keyword_matches = process.extractBests(wd.lower().strip(","), limit=1, choices=list(map(lambda x: x.strip('\"'), KEYWORDS)), score_cutoff=75)
    #table_matches = process.extractBests(wd.lower().strip(","), limit=1, choices=list(map(lambda x: x.strip('\"'), corrs['table'])), score_cutoff=85)
    #col_matches = process.extractBests(wd.lower().strip(","), limit=1, choices=list(map(lambda x: x.strip('\"'), corrs['cols'])), score_cutoff=85)
    #cmds_aggs_matches = process.extractBests(wd.lower().strip(","), limit=1, choices=list(map(lambda x: x.strip('\"'), corrs['cmds_aggs'])), score_cutoff=75)
    
    #wd = LEMMATIZER.lemmatize(wd)
    res = process.extractBests(wd.lower().strip(","), limit=1, choices=list(map(lambda x: x.strip('\"'), corrs['table'] + corrs['cols'] + corrs['cmds_aggs'] + KEYWORDS)), score_cutoff=75)


    try:
        float(wd.strip(",.;"))
        NUMBER_STORE.append(wd.strip(",.;"))
        wd = "<number>"
        return wd
    except:
        pass
    
    if "'" in wd:
        wd = re.match('[^a-zA-Z0-9\s-]*([\w\s-]+)[^a-zA-Z0-9\s-]*', wd).group(1)
        STRING_STORE.append(wd)
        wd = "<string>"
    elif wd in ['>', '<', '>=', '<=', '=']:
        pass
    elif len(res):
        wd = res[0][0]
    else:
        try:
            float(wd)
            NUMBER_STORE.append(wd)
            wd = "<number>"
        except:
            wd = ""
            pass
        
    return wd
    


def get_parser(schema_description, table, print_cfg=True):


    def further_split(l):
        rl = []
        for e in l:
            e = e.replace('"', '')
            rl += [' '.join([ '\"' + ee + '\"' for ee in e.split(" ")])]
        return rl 

    collection = table
    col_cat = {'CAT' : [], 'NUM':[], 'NOMCAT':[], 'ORDCAT':[]}

    #query = "select all names of races happened in year 2009"
    #query = map(_preprocess, query.split())
    #query = " ".join(list(query))
                    
    
    for col in schema_description[collection]:
        _type = schema_description[collection][col][2]
        
        if _type  == 'cat':
            col_cat['CAT'] +=  (['\"`' + col + '`\"'] if len(col.split())>1 else ['\"' + col + '\"'])
        elif _type  == 'nominal-cat':
            col_cat['NOMCAT'] += (['\"`' + col + '`\"'] if len(col.split())>1 else ['\"' + col + '\"'])
        elif _type  == 'ordinal-cat':
            col_cat['ORDCAT'] += (['\"`' + col + '`\"'] if len(col.split())>1 else ['\"' + col + '\"'])
        else:
            col_cat['NUM'] += (['\"`' + col + '`\"'] if len(col.split())>1 else ['\"' + col + '\"'])
    

    # Initialize the Datamuse client
    api = Datamuse()

    # Query for synonyms of a word, e.g., 'happy'
    cmd_wds = ['get', 'fetch', 'find', 'select', 'grab', 'extract']
    cmds = set()
    for cmd_wd in cmd_wds:
        synonyms = api.words(topics='select, find, search, get', rel_syn=cmd_wd, max=10)
        for word in synonyms:
            cmds.add('\"' + word['word'].replace(" ", "-") + '\"')
    for word in cmd_wds:
        cmds.add('\"' + word.replace(" ","-") + '\"')
    cmds = list(cmds)
    
    agg_wds = {'sums' : ['summation'], 'mins' : ['minimum'], 'maxs' : ['maximum'], 'medians' : ['median'], 'avgs' : ['average'], 'counts' : ['count'], 'variances' : ['variance'], 'stds' : ['standard deviation'], 'distincts' : ['distinct', 'unique']}
    aggs = set()
    for agg_ref in agg_wds:
        agg_ref_res = set()
        for agg_base in agg_wds[agg_ref]:
            synonyms = api.words(rel_syn=agg_base, topics='aggregation, statistics', max=10)
            for word in synonyms:
                agg_ref_res.add('\"' + word['word'].replace(" ", "-") + '\"')
            agg_ref_res.add('\"' + agg_base.replace(" ", "-") + '\"')
        agg_wds[agg_ref] = list(agg_ref_res)
        aggs = aggs.union(agg_ref_res)

    aggs = list(aggs)
   
    # Print the synonyms

    cats = ' | '.join(col_cat['CAT'])
    if not(cats):
        cats = '\"<default>\"'

    nums = ' | '.join(col_cat['NUM'])
    if not(nums):
        nums =  '\"<default>\"'

    nomcats = ' | '.join(col_cat['NOMCAT'])
    if not(nomcats):
        nomcats = '\"<default>\"'
    
    ordcats = ' | '.join(col_cat['ORDCAT'])
    if not(ordcats):
        ordcats = '\"<default>\"'
    

    table = '\"' + table + '\"'
    cfg_string = f"""
    S -> CMD "*" TABLE CLAUSE | CMD COLUMN TABLE CLAUSE | CMD AGG COLUMN TABLE CLAUSE | CMD "*" CLAUSE TABLE | CMD COLUMN CLAUSE TABLE| CMD AGG COLUMN CLAUSE TABLE | CLAUSE CMD COLUMN TABLE | CLAUSE CMD AGG COLUMN TABLE | CLAUSE CMD COLUMN TABLE CLAUSE | CLAUSE CMD AGG COLUMN TABLE CLAUSE | TABLE CMD COLUMN CLAUSE | TABLE CMD AGG COLUMN CLAUSE | TABLE CLAUSE CMD COLUMN | TABLE CLAUSE CMD AGG COLUMN | TABLE CLAUSE CMD COLUMN CLAUSE | TABLE CLAUSE CMD AGG COLUMN CLAUSE | CLAUSE CMD COLUMN CLAUSE TABLE | CLAUSE CMD AGG COLUMN CLAUSE TABLE | CLAUSE TABLE CMD COLUMN | CLAUSE TABLE CMD AGG COLUMN | CLAUSE TABLE CMD COLUMN CLAUSE | CLAUSE TABLE CMD AGG COLUMN CLAUSE
    CLAUSE -> "where" COLUMN NOP VALUE CLAUSE | "group" "by" COLUMN CLAUSE | "order" "by" COLUMN CLAUSE 
    CLAUSE -> 
    COLUMN -> CAT COLUMN | NUM COLUMN | NOMCAT COLUMN | ORDCAT COLUMN | CAT AGG COLUMN | NUM AGG COLUMN | NOMCAT AGG COLUMN | ORDCAT AGG COLUMN
    COLUMN -> CAT | NUM | NOMCAT | ORDCAT
    CAT -> {cats}
    NUM -> {nums}
    NOMCAT -> {nomcats}
    ORDCAT -> {ordcats}
    CMD -> {' | '.join(cmds)}
    AGG -> SUM | MIN | MAX | MEDIAN | AVG | COUNT | VARIANCE | STDDEV | DISTINCT
    SUM -> {' | '.join(agg_wds['sums'])}
    MIN -> {' | '.join(agg_wds['mins'])}
    MAX -> {' | '.join(agg_wds['maxs'])}
    MEDIAN -> {' | '.join(agg_wds['medians'])}
    AVG -> {' | '.join(agg_wds['avgs'])}
    COUNT -> {' | '.join(agg_wds['counts'])}
    VARIANCE -> {' | '.join(agg_wds['variances'])}
    STDDEV -> {' | '.join(agg_wds['stds'])}
    DISTINCT -> {' | '.join(agg_wds['distincts'])}
    NOP -> ">" | "<" | ">=" | "<=" | "="
    TABLE -> {table}
    VALUE -> "<number>" | "<string>"
    """
    if print_cfg:
        print("="*50 + ' CFG ' + "="*50)
        print(cfg_string)
        print("="*105)

    grammar = CFG.fromstring(cfg_string)
    # Define a parser
    parser = nltk.ChartParser(grammar, trace=0)

    return cfg_string, parser, {'cols' : list(set(col_cat['CAT'] + col_cat['NUM'] + col_cat['NOMCAT'] + col_cat['ORDCAT'])), 'cmds_aggs' : list(set(cmds + aggs)), 'table' : [table]}

def restructure_query(query, corrs):
    global NUMBER_STORE, STRING_STORE
    NUMBER_STORE = []
    STRING_STORE = []

    corrected_query = " ".join([wd for wd in list(map(lambda x: _preprocess(x, corrs), _split_ignore_quoticks(query))) if wd])
    return corrected_query



def query_sql(db='SQL/thrombosis_prediction/thrombosis_prediction.sqlite', q="select *"):

    # Create a SQL connection to our SQLite database
    con = sqlite3.connect(db)
    cur = con.cursor()
    results = []
    for row in cur.execute(q):
        results += [row]
    # Be sure to close the connection
    con.close()
    return results

def query_nosql(dbname='SQL/thrombosis_prediction/thrombosis_prediction.sqlite', q="select *"):
    results = list(eval('client[dbname].' + q))
    return results

 

def parse_tree_to_sql(tree):
    cmd_clause = []
    condition_clause = []
    table_clause = ""
    ns_head = 0
    ss_head = 0

    select_effect = False
    agg_effect = False
    clause_effect = False
    def traverse_tree(node):
        nonlocal table_clause, condition_clause, select_effect, agg_effect, clause_effect, ns_head, ss_head
        if isinstance(node, Tree):
            label = node.label()

            if label == 'CMD':
                # 'select' keyword handling
                select_effect = True
                clause_effect = False
                cmd_clause.append(f'SELECT ')
                pass  # Already implied in SQL

            elif label == 'AGG':
                # Add aggregation function
                agg_effect = True
              
            
            elif label in ["SUM", "MIN", "MAX", "MEDIAN", "AVG", "COUNT", "VARIANCE", "STDDEV", "DISTINCT"]:
                    #agg = AGGREGATE_MAP[label]
                    cmd_clause.append(f"{label}(")
       

            elif label == 'COLUMN':
                # Add column names
                if clause_effect:
                    column_name = f'{node[0][0]}'
                    condition_clause.append(column_name)
                    pass
                elif select_effect:
                    columns = []
                    for child in node:
                        if isinstance(child, Tree) and child.label() in ['CAT', 'NUM', 'ORDCAT', 'NOMCAT']:
                            if agg_effect:
                                agg_effect = False
                                columns.append(f'{child[0]}), ')
                            else:
                                columns.append(f'{child[0]}, ')
                        else:
                            pass
                    if columns:
                        cmd_clause.extend(columns)
                else:
                    pass

            elif label == 'TABLE':
                table_clause = node[0]  # Table name

            elif label == 'CLAUSE':
                # Build where clause
                clause_effect = True 
                if len(node):
                    i = 0
                    while not(isinstance(node[i], Tree)):
                        condition_clause.append(node[i].upper())
                        i += 1
            elif label == 'NOP':
                condition_clause.append(node[0])
            
            elif label == 'VALUE':
                clause_effect = False
                if node[0] == "<number>":
                    condition_clause.append(NUMBER_STORE[ns_head])
                    ns_head += 1
                else:
                    condition_clause.append("'" + STRING_STORE[ss_head] + "'")
                    ss_head += 1
                
        
            # Recurse into children nodes
            for child in node:
                traverse_tree(child)

    # Start tree traversal
    traverse_tree(tree)

    # Construct SQL statement
    cmd_part = ''.join(cmd_clause) #if cmd_clause else '*'
    condition_part = ' '.join(condition_clause) 
    select_part = cmd_part.strip(", ")
    sql_query = f"{select_part} FROM {table_clause} {condition_part}"
    return sql_query



import re
from typing import Dict, Any, Union, List

class SQLToMongoConverter:
    def __init__(self):
        # Mapping of SQL aggregation functions to MongoDB equivalents
        self.agg_mapping = {
            "COUNT": "$sum",  # Using $sum: 1 for count
            "AVG": "$avg",
            "SUM": "$sum",
            "MIN": "$min",
            "MAX": "$max",
            "MEDIAN": "$avg",
            "STDDEV": "$stdDevPop",
            "VARIANCE": "$stdDevSamp",
            "DISTINCT" : "$addToSet"
        }
        
        # Mapping of SQL operators to MongoDB equivalents
        self.operator_mapping = {
            ">": "$gt",
            "<": "$lt",
            ">=": "$gte",
            "<=": "$lte",
            "=": "$eq"
        }

    def parse_value(self, value: str) -> Union[int, float, str]:
        """Parse value while preserving integer type"""
        if "'" in value:
            return value.strip('"\'')

        value = value.strip('"\'')        
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    def parse_select_part(self, select_part: str, group_by: str = None) -> Dict[str, Any]:
        """Parse the SELECT part of the SQL query"""
        if select_part == "*":
            return {}
        
        # Check for aggregation with brackets pattern: COUNT(ID), SUM(Price) etc.
        agg_match = re.match(r'(\w+)\((.*?)\)', select_part)
        if agg_match:
            agg_func, column = agg_match.groups()
            if agg_func in self.agg_mapping:
                # If there's a GROUP BY clause, use it as _id
                if group_by:
                    return {
                        "$group": {
                            "_id": f"${group_by}",
                            "result": {
                                self.agg_mapping[agg_func]: f"${column.replace('`', '')}" if agg_func != "COUNT" else 1
                            }
                        }
                    }
                # If no GROUP BY, use null as _id
                return {
                    "$group": {
                        "_id": None,
                        "result": {
                            self.agg_mapping[agg_func]: f"${column.replace('`', '')}" if agg_func != "COUNT" else 1
                        }
                    }
                }
        
        # Simple column selection
        columns = [col.strip() for col in select_part.split(",")]
        return {"$project": {col.replace("`", ""): 1 for col in columns}}

    def parse_where_clause(self, where_clause: str) -> Dict[str, Any]:
        """Parse the WHERE clause of the SQL query"""
        if not where_clause:
            return {}
            
        matches = re.match(r'\s*(\`?\w+(?:\s+\w+)*\`?)\s*([<>=]+)\s*([^,;]+)', where_clause)
        if not matches:
            return {}
            
        column, operator, value = matches.groups()
        value = self.parse_value(value)
        
        return {
            column.replace("`", ""): {
                self.operator_mapping[operator]: value
            }
        }

    def parse_group_by(self, group_clause: str) -> str:
        """Parse the GROUP BY clause and return column name"""
        if not group_clause:
            return None
        return group_clause.strip()

    def parse_order_by(self, order_clause: str) -> Dict[str, Any]:
        """Parse the ORDER BY clause"""
        if not order_clause:
            return {}
            
        column = order_clause.strip()
        return {
            "$sort": {
                column.replace("`", ""): 1
            }
        }

    def convert_to_mongo(self, sql_query: str) -> List[Dict[str, Any]]:
        """Convert SQL query to MongoDB query"""
        # Extract main parts using regex
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql_query)
        table_match = re.search(r'FROM\s+(\`?\w+\`?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+WHERE\s+|\s*$)', sql_query)
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|$)', sql_query)
        group_match = re.search(r'GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|$)', sql_query)
        order_match = re.search(r'ORDER\s+BY\s+(.+?)$', sql_query)
        
        pipeline = []
        
        # Parse WHERE clause if exists
        if where_match:
            match_stage = self.parse_where_clause(where_match.group(1))
            if match_stage:
                pipeline.append({"$match": match_stage})
        
        # Get group by column if exists
        group_by_column = None
        if group_match:
            group_by_column = self.parse_group_by(group_match.group(1))
        
        # Parse SELECT part with group by information
        if select_match:
            project_stage = self.parse_select_part(select_match.group(1), group_by_column)
            if project_stage:
                pipeline.append(project_stage)
        
        # Parse ORDER BY if exists
        if order_match:
            sort_stage = self.parse_order_by(order_match.group(1))
            if sort_stage:
                pipeline.append(sort_stage)
        
        return table_match.group(1) + '.aggregate(' + str(pipeline) + ')'


    



