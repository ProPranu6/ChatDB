from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
import os
import zipfile
import shutil
from io import BytesIO
from fastapi import FastAPI
from engine import client, data_explore_display, data_explore, get_parser, generate_sql, restructure_query, parse_tree_to_sql, query_sql, query_nosql, SQLToMongoConverter
from pydantic import BaseModel
from warnings import filterwarnings
import json
filterwarnings('ignore')


from fastapi.staticfiles import StaticFiles
from bson import ObjectId


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return super().default(o)
class DBInfo(BaseModel):
    db_path: str

class QueryInfo(BaseModel):
    db_path : str 
    table : str 
    query : str
    
class QueryRequest(BaseModel):
    query: str

converter = SQLToMongoConverter().convert_to_mongo
app = FastAPI()

# Mount the static directory (where index.html is stored)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Path to save uploaded files
UPLOAD_DIR_NOSQL = "./test/NoSQL"
UPLOAD_DIR_SQL = "./test/SQL"

# In-memory store to track uploaded databases and relevant information
MEMORY = {'SQL': {}, 'NoSQL': {}}
MEMORY_STORE = {"schema": "", 'parser': "", "corrs": ""}
last_uploaded_db = None  # Track the type of last uploaded database ('sql' or 'nosql')

# Create the upload directory if it doesn't exist
if not os.path.exists(UPLOAD_DIR_NOSQL):
    os.makedirs(UPLOAD_DIR_NOSQL)

if not os.path.exists(UPLOAD_DIR_SQL):
    os.makedirs(UPLOAD_DIR_SQL)

@app.post("/upload/nosql")
async def upload_file_nosql(file: UploadFile = File(...)):
    global last_uploaded_db

    # Save the uploaded file to the disk
    file_path = os.path.join(UPLOAD_DIR_NOSQL, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Check if the file is a zip file
    if file.filename.endswith(".zip"):
        unzip_folder = os.path.join(UPLOAD_DIR_NOSQL, file.filename[:-4])  # Create a folder for unzipped contents
        if not os.path.exists(unzip_folder):
            os.makedirs(unzip_folder)
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(unzip_folder)
        os.remove(file_path)
        last_uploaded_db = 'nosql'  # Update last uploaded db type
        # List the files in the NoSQL directory
        files = os.listdir(UPLOAD_DIR_NOSQL)
        return {"message": f"File uploaded and unzipped successfully. Files extracted to {unzip_folder}", "databases": files}

    else:
        last_uploaded_db = 'nosql'  # Update last uploaded db type
        # List the files in the NoSQL directory
        files = os.listdir(UPLOAD_DIR_NOSQL)
        return {"message": f"File uploaded successfully: {file.filename}", "databases": files}


@app.post("/upload/sql")
async def upload_file_sql(file: UploadFile = File(...)):
    global last_uploaded_db

    # Save the uploaded file to the disk
    file_path = os.path.join(UPLOAD_DIR_SQL, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Check if the file is a zip file
    if file.filename.endswith(".zip"):
        unzip_folder = os.path.join(UPLOAD_DIR_SQL, file.filename[:-4])  # Create a folder for unzipped contents
        if not os.path.exists(unzip_folder):
            os.makedirs(unzip_folder)
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(unzip_folder)
        os.remove(file_path)

        return {"message": f"File uploaded and unzipped successfully. Files extracted to {unzip_folder}"}
    else:
        last_uploaded_db = 'sql'  # Update last uploaded db type
        files = os.listdir(UPLOAD_DIR_SQL)
        return {"message": f"File uploaded successfully: {file.filename}", "databases": files}

@app.get("/list/nosql")
async def db_explore_nosql():
    return {"message": f"{os.listdir(UPLOAD_DIR_NOSQL)}"}

@app.get("/list/sql")
async def db_explore_sql():
    return {"message": f"{os.listdir(UPLOAD_DIR_SQL)}"}

@app.post("/schema")
async def data_explore_api(db_info: DBInfo):
    return JSONResponse({"message": data_explore_display(db_info.db_path)})

@app.post("/sq/sql")
async def generate_sample_q_sql(db_info: DBInfo):
    schema_description = data_explore(db_info.db_path)
    q = generate_sql(schema_description)
    return JSONResponse({"message": q})

@app.post("/sq/nosql")
async def generate_sample_q_nosql(db_info: DBInfo):
    schema_description = data_explore(db_info.db_path)
    q = list(map(converter, generate_sql(schema_description)))
    return JSONResponse({"message": q})

# SQL query handling
@app.post("/query/sql")
async def query_sql_api(query_info: QueryInfo):
    global MEMORY, MEMORY_STORE, last_uploaded_db
    if last_uploaded_db != 'sql':
        raise HTTPException(status_code=400, detail="No SQL database uploaded")

    db_path, table, query = query_info.db_path, query_info.table, query_info.query
    if db_path in MEMORY["SQL"]:
        schema_description = MEMORY_STORE['schema']
        if table == MEMORY["SQL"][db_path]:
            parser, corrs = MEMORY_STORE['parser'], MEMORY_STORE['corrs']
        else:
            parser, corrs = get_parser(schema_description, table)
    else:
        schema_description = data_explore(db_path)
        parser, corrs = MEMORY_STORE['parser'], MEMORY_STORE['corrs']

    query = restructure_query(query, corrs)
    tokens = query.split()
    tree = next(parser.parse(tokens))
    sql = parse_tree_to_sql(tree)
    results = query_sql(os.path.join(db_path, os.path.basename(db_path), '.sqlite'), sql)

    MEMORY["SQL"] = {db_path: table}
    MEMORY_STORE["schema"] = schema_description
    MEMORY_STORE['parser'] = parser
    MEMORY_STORE['corrs'] = corrs

    serialized_results = json.loads(JSONEncoder().encode(results))
    result_string = f"SQL Query: {sql}" + " "+f"Data: {json.dumps(serialized_results)}"

    return JSONResponse({"message": result_string})

# NoSQL query handling
@app.post("/query/nosql")
async def query_nosql_api(query_info: QueryInfo):
    global MEMORY, MEMORY_STORE, last_uploaded_db
    if last_uploaded_db != 'nosql':
        raise HTTPException(status_code=400, detail="No NoSQL database uploaded")

    db_path, table, query = query_info.db_path, query_info.table, query_info.query
    if db_path in MEMORY["NoSQL"]:
        schema_description = MEMORY_STORE['schema']
        if table == MEMORY["NoSQL"][db_path]:
            parser, corrs = MEMORY_STORE['parser'], MEMORY_STORE['corrs']
        else:
            parser, corrs = get_parser(schema_description, table)
    else:
        schema_description = data_explore(db_path)
        parser, corrs = get_parser(schema_description, table)

    query = restructure_query(query, corrs)
    tokens = query.split()
    tree = next(parser.parse(tokens))
    sql = parse_tree_to_sql(tree)
    nosql = converter(sql)
    results = query_nosql(os.path.basename(db_path), nosql)

    MEMORY["NoSQL"] = {db_path: table}
    MEMORY_STORE["schema"] = schema_description
    MEMORY_STORE['parser'] = parser
    MEMORY_STORE['corrs'] = corrs

    # Serialize results with JSONEncoder
    serialized_results = json.loads(JSONEncoder().encode(results))
    result_string = f"NoSQL Query: {nosql}" + " "+f"Data: {json.dumps(serialized_results)}"

    return JSONResponse({"message": result_string})

@app.get("/")
async def root():
    return {"message": "Try the '/upload/sql', '/upload/nosql', '/list/nosql', '/list/sql', '/schema', '/sq/sql', '/sq/nosql', '/parse/sql', '/parse/nosql' APIs."}
