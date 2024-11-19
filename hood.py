from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
import os
import zipfile
import shutil
from io import BytesIO
from fastapi import FastAPI
from engine import client, data_explore, get_parser, generate_sql, restructure_query, parse_tree_to_sql, query_sql, query_nosql, SQLToMongoConverter
from pydantic import BaseModel
from warnings import filterwarnings
filterwarnings('ignore')


class DBInfo(BaseModel):
    db_path: str

class QueryInfo(BaseModel):
    db_path : str 
    table : str 
    query : str
    

converter = SQLToMongoConverter().convert_to_mongo
app = FastAPI()

# Path to save uploaded files
UPLOAD_DIR_NOSQL = "./test/NoSQL"
UPLOAD_DIR_SQL = "./test/SQL"

MEMORY = {'SQL' : {}, 'NoSQL' : {}}
MEMORY_STORE = {"schema" : "", 'parser' : "", "corrs" : ""}

# Create the upload directory if it doesn't exist
if not os.path.exists(UPLOAD_DIR_NOSQL):
    os.makedirs(UPLOAD_DIR_NOSQL)

if not os.path.exists(UPLOAD_DIR_SQL):
    os.makedirs(UPLOAD_DIR_SQL)

@app.post("/upload/nosql")
async def upload_file_api(file: UploadFile = File(...)):
    # Save the uploaded file to the disk
    file_path = os.path.join(UPLOAD_DIR_NOSQL, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Check if the file is a zip file
    if file.filename.endswith(".zip"):
        # Unzip the file
        unzip_folder = os.path.join(UPLOAD_DIR_NOSQL, file.filename[:-4])  # Create a folder for unzipped contents
        if not os.path.exists(unzip_folder):
            os.makedirs(unzip_folder)

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(unzip_folder)
        os.remove(file_path)

        return {"message": f"File uploaded and unzipped successfully. Files extracted to {unzip_folder}"}
    else:
        return {"message": f"File uploaded successfully: {file.filename}"}
    

@app.post("/upload/sql")
async def upload_file_api(file: UploadFile = File(...)):
    # Save the uploaded file to the disk
    file_path = os.path.join(UPLOAD_DIR_SQL, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Check if the file is a zip file
    if file.filename.endswith(".zip"):
        # Unzip the file
        unzip_folder = os.path.join(UPLOAD_DIR_SQL, file.filename[:-4])  # Create a folder for unzipped contents
        if not os.path.exists(unzip_folder):
            os.makedirs(unzip_folder)

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(unzip_folder)
        os.remove(file_path)

        return {"message": f"File uploaded and unzipped successfully. Files extracted to {unzip_folder}"}
    else:
        return {"message": f"File uploaded successfully: {file.filename}"}
    

@app.get("/list/nosql")
async def db_explore_api():
    return {"message" : f"{os.listdir(UPLOAD_DIR_NOSQL)}"}
 
@app.get("/list/sql")
async def db_explore_api():
    return {"message" : f"{os.listdir(UPLOAD_DIR_SQL)}"}

@app.post("/schema")
async def data_explore_api(db_info : DBInfo):
    return JSONResponse({"message" : data_explore(db_info.db_path)})

@app.post("/sq/sql")
async def generate_sample_q_api(db_info:DBInfo):
    schema_description=data_explore(db_info.db_path)
    q = generate_sql(schema_description)
    return JSONResponse({"message" : q})

@app.post("/sq/nosql")
async def generate_sample_q_api(db_info:DBInfo):
    schema_description=data_explore(db_info.db_path)
    q = list(map(converter, generate_sql(schema_description)))
    return JSONResponse({"message" : q})

@app.post("/query/sql")
async def query_sql_api(query_info:QueryInfo):
    global MEMORY, MEMORY_STORE

    db_path, table, query = query_info.db_path, query_info.table, query_info.query
    if db_path in MEMORY["SQL"]:
        schema_description = MEMORY_STORE['schema']
        if table == MEMORY["SQL"][db_path]:
            parser, corrs = MEMORY_STORE['parser'], MEMORY_STORE['corrs']
        else:
            parser, corrs = get_parser(schema_description, table)
    else:
        schema_description=data_explore(db_path)
        parser, corrs = MEMORY_STORE['parser'], MEMORY_STORE['corrs']


    query = restructure_query(query, corrs)
    tokens = query.split()
    tree = next(parser.parse(tokens))
    sql = parse_tree_to_sql(tree)
    results = query_sql(os.path.join(db_path, os.path.basename(db_path), '.sqlite'), sql)

    MEMORY["SQL"] = {db_path : table}
    MEMORY_STORE["schema"] = schema_description
    MEMORY_STORE['parser'] = parser 
    MEMORY_STORE['corrs'] = corrs 

    return JSONResponse({"message" : results})

@app.post("/query/nosql")
async def query_nosql_api(query_info:QueryInfo):
    global MEMORY, MEMORY_STORE
    db_path, table, query = query_info.db_path, query_info.table, query_info.query
    if db_path in MEMORY["NoSQL"]:
        schema_description = MEMORY_STORE['schema']
        if table == MEMORY["NoSQL"][db_path]:
            parser, corrs = MEMORY_STORE['parser'], MEMORY_STORE['corrs']
        else:
            parser, corrs = get_parser(schema_description, table)
    else:
        schema_description=data_explore(db_path)
        parser, corrs = get_parser(schema_description, table)


    query = restructure_query(query, corrs)
    tokens = query.split()
    tree = next(parser.parse(tokens))
    sql = parse_tree_to_sql(tree)
    nosql = converter(sql)
    results = query_nosql(os.path.basename(db_path), nosql)
    
    MEMORY["NoSQL"] = {db_path : table}
    MEMORY_STORE["schema"] = schema_description
    MEMORY_STORE['parser'] = parser 
    MEMORY_STORE['corrs'] = corrs 

    return JSONResponse({"message" : results})




@app.get("/")
async def root():
    return {"message": "try '/upload/sql', '/upload/nosql', '/list/nosql', '/list/sql', '/schema', '/sq/sql', '/sq/nosql', '/parse/sql', '/parse/nosql' APIs"}