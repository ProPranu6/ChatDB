from engine import client, data_explore, get_parser, generate_sql, restructure_query, parse_tree_to_sql, query_sql, query_nosql, SQLToMongoConverter
from warnings import filterwarnings
filterwarnings('ignore')

class NoSQLTestCases:
    def __init__(self, db_name='formula_1', max_sample=3):
        self.db_name = db_name
        self.schema_description=data_explore(f'NoSQL/{db_name}', print_schema=False)
        self.max_sample = max_sample
        self.t = [
    # constructorStandings
    ("constructorStandings", "Fetch the minimum points from constructorStandings, grouped by constructorStandingsId."),
    ("constructorStandings", "Get the minimum position from constructorStandings where points = 7."),
    ("constructorStandings", "Get the mean of points from constructorStandings."),
    ("constructorStandings", "Fetch the total points from constructorStandings."),
    ("constructorStandings", "Get the summation of points from constructorStandings, that are grouped by _id."),
    
    # circuits
    ("circuits", "Fetch the minimum long from circuits, ordered by lat."),
    ("circuits", "Fetch the variance in long from circuits where circuitId > 50"),
    
    # constructors
    ("constructors", "Fetch _id from constructors."),
    
    # races
    ("races", "Get year from races."),
    ("races", "Fetch round from races.")
]

        
    def __call__(self, t):
        print("#"*100)
        try:
            table, query = t
            print("NL Query : ", query)
            self.query = query
            parser, corrs = get_parser(self.schema_description, table, print_cfg=False)
            self.corrs = corrs
            query = restructure_query(query, corrs)
            print("Reconstructed Query : ", query)
            self.reconstructed_query = query
            tokens = query.split()
            tree = next(parser.parse(tokens))
            self.tree = tree
            print("Parse Tree : ", tree)
            sqlt = parse_tree_to_sql(tree)
            self.sql = sqlt
            print("SQL : ", sqlt)
            nosql = SQLToMongoConverter().convert_to_mongo(sqlt)
            self.nosql = nosql
            print("NoSQL : ", nosql)
            res = query_nosql('formula_1', nosql)[:self.max_sample]
            print("Results : ", res)
            return res
        except:
            res = []
            print("Results : ", res)
            return []
        
class SQLTestCases:
    def __init__(self, db_name='thrombosis_prediction', max_sample=3):
        self.db_name = db_name
        self.schema_description=data_explore(f'SQL/{db_name}', print_schema=False)
        self.max_sample = max_sample
        self.t = [
    # Examination table
    ("Examination", "Get the average of ANA where Thrombosis = 1 from Examination."),
    ("Examination", "Retrieve the sum of ANA where Thrombosis = 2 in Examination."),
    ("Examination", "Fetch unique values of Symptoms where Thrombosis = 3 from Examination."),
    ("Examination", "Fetch distinct Diagnosis values where Thrombosis = 2 in Examination."),


    # Laboratory table
    ("Laboratory", "Fetch the maximum value of GOT where GOT > 100 from Laboratory."),
    ("Laboratory", "Get the average of CPK where TG > 100 from Laboratory."),
    ("Laboratory", "Retrieve the average value of ALB where HGB > 12 from Laboratory."),
    ("Laboratory", "Get the total count of RBC where T-BIL > 0.2 in Laboratory."),
    ("Laboratory", "Fetch the range of UA values where CRP = 2 in Laboratory."),
    ("Laboratory", "Retrieve the minimum value of ALP where ALP > 15 in Laboratory."),


    # Patient table
    ("Patient", "Fetch the total count of PatientID where SEX = 1 from Patient."),
    ("Patient", "Get all unique values of SEX where ID = 4060811 from Patient."),
    ("Patient", "Fetch all non-null Description values where PatientID = 1124385 from Patient.")

]
   
    def __call__(self, t):
        print("#"*100)
        try:
            table, query = t
            print("NL Query : ", query)
            self.query = query
            parser, corrs = get_parser(self.schema_description, table, print_cfg=False)
            query = restructure_query(query, corrs)
            print("Reconstructed Query : ", query)
            self.reconstructed_query = query
            tokens = query.split()
            tree = next(parser.parse(tokens))
            print("Parse Tree : ", tree)
            self.tree = tree
            sql = parse_tree_to_sql(tree)
            self.sql = sql
            print("SQL : ", sql)
            res = query_sql(f'SQL/{self.db_name}/{self.db_name}.sqlite', sql)[:self.max_sample]
            print("Results : ", res)
            return res
        except:
            print("Results : ", [])
            return []

