from engine import client, data_explore, get_parser, generate_sql, restructure_query, parse_tree_to_sql, query_sql, query_nosql, SQLToMongoConverter, _split_ignore_quoticks as tokenizer
from warnings import filterwarnings
filterwarnings('ignore')

class NoSQLTestCases:
    #changed default db_name to 'default'
    def __init__(self, db_name='default', max_sample=3, debug=True):
        self.db_name = db_name
        self.schema_description=data_explore(f'NoSQL/{db_name}', print_schema=False)
        self.max_sample = max_sample
        self.debug = debug

        #creating options for multiple databases
        if self.db_name == 'formula_1':
            self.t = [
        # constructorStandings
        ("constructorStandings", "where points = 7 in constructorStandings, get the minimum position"),
        ("constructorStandings", "get me a summation of wins in `constructor standings` where position placed is > 3 grouped by constructorid"),
        ("constructorStandings", "extract all distinct raceids happned in constructiorstandings"),
        ("constructorStandings", "get maximum points that is scored, grouped by constructorid from constructorstandings"),
        
        # circuits
        ("circuits", "Fetch the minimum long from circuits, ordered by lat."),
        ("circuits", "Fetch the variance in long from circuits where circuitId > 50"),
        ('circuits', 'find all distinct locations grouped by country in circuits'),
        ("circuits", "find mean lat of circuits where the host country is = 'France'"),
        
        # constructors
        ("constructors", "Fetch _id from constructors."),
        ("constructors", "find the nationality and the name of the car from constructors info"),
        ("constructors", "for where car name is = 'Ferrari', get me its website url from the constructors table"),
        ("constructors", "find what are all the names of the cars where nationality is = 'Italian' in constructors"),

        # races
        ("races", "get me circuitids of all races where the rounds that happened there are > 10"),
        ("races", "From races, what cna you get as the smallest time for where the name of the venue is = 'Australian Grand Prix'"),
        ("races", "From races, get me the maximum rounds happened where the venue is named = 'Monaco Grand Prix'"),
        ("races", "Find me the website ursl for all races where they happened in year = 2009 and group them by name of venue")

    ]
        elif self.db_name == 'debit_card_specialization':
            self.t = [
                #customers
                ("customers","Fetch unique values of CustomerID from Customers where segment = 'SME'."),
                ("customers", "Fetch unique values of CustomerID from customers where Currency = 'EUR'."),

                #gasstations
                ("gasstations","Fetch unique values of ChainID from gasstations where Segment = 'Premium'."),
                ("gasstations","Get unique values of GasStationID from gasstations where Country = 'CZE'."),

                #products
                ("products","Get Description from products where ProductID = 5."),

                #transactions_1k
                ("transactions_1k","Fetch the minimum Price from transactions_1k."),
                ("transactions_1k","Fetch the variance in Amount from transactions_1k where Price > 100."),
                ("transactions_1k","Get CardID from transactions_1k.")
            ]
        elif self.db_name == 'california_schools':
            self.t = [
    ("satscores", "Grab AvgScrMath from the satscores dataset where cds = '1100170000000'."),
    ("satscores", "Pick NumTstTakr out of satscores where cname = 'Alameda'."),
    ("satscores", "Get distinct cds values from satscores."),
    ("satscores", "Retrieve unique rtype values from satscores where AvgScrRead > 500."),
    ("frpm", "Pull out `School Name` from frpm where CDSCode = '01100170109835'."),
    ("frpm", "Extract `Percent (%) Eligible FRPM (Ages 5-17)` from frpm where `School Name` = 'Envision Academy for Arts & Technology'."),
    ("frpm", "Fetch all distinct `Charter School Numbers` in frpm."),
    ("frpm", "Grab distinct `School Type` from frpm where `Percent (%) Eligible Free (K-12)` > 0.5."),
    ("schools", "Fetch AdmEmail1 from schools where CDSCode = '01100170000000'."),
    ("schools", "Extract CharterNum from schools where Charter = 1."),
    ("schools", "Select distinct District names from schools."),
    ("schools", "Pull unique StatusType values from schools where State = 'CA'.")
]
        
    def __call__(self, t):
        print("#"*100)
        try:
            table, query = t
            print("NL Query : ", query)
            self.query = query
            cfg_string, parser, corrs = get_parser(self.schema_description, table, print_cfg=False)
            self.corrs = corrs
            self.cfg = cfg_string
            query = restructure_query(query, corrs)
            print("Reconstructed Query : ", query)
            self.reconstructed_query = query
            tokens = tokenizer(query)
            tree = next(parser.parse(tokens))
            self.tree = tree
            print("Parse Tree : ", tree)
            sqlt = parse_tree_to_sql(tree)
            self.sql = sqlt
            print("SQL : ", sqlt)
            nosql = SQLToMongoConverter().convert_to_mongo(sqlt)
            self.nosql = nosql
            print("NoSQL : ", nosql)
            res = query_nosql(self.db_name, nosql)[:self.max_sample]
            print("Results : ", res)
            return res
        except Exception as E:
            if self.debug:
                print(f"[ERROR] {E}")
            res = []
            print("Results : ", res)
            return []
        
class SQLTestCases:
    #changed default db name to default
    def __init__(self, db_name='default', max_sample=3, debug=True):
        self.db_name = db_name
        self.schema_description=data_explore(f'SQL/{db_name}', print_schema=False)
        self.max_sample = max_sample
        self.debug = debug
        if self.db_name == 'thrombosis_prediction':
            self.t = [
        # Examination table
        ("Examination", "From the examimation, what can you find as the average ANA for where Thrombosis = 1"),
        ("Examination", "According to examinations data, what can you get as the diagnosis to be used for where sympotms = 'AMI'"),
        ("Examination", "get me all the distinct symptoms found and their `ANA Pattern`, according to the examinations, where thrombosis = 3 grouping it by `ANA Pattern`"),
        ("Examination", "find me all the `examination dates` from the examination records for where `aCL IgM` > 10"),


        # Laboratory table
        ("Laboratory", "get me the maximum value for GOT, CRP from the laboratory records where GOT > 100 and group this by CRP."),
        ("Laboratory", "Get the average of CPK where TG > 100 from Laboratory."),
        ("Laboratory", "Retrieve the average value of ALB where HGB > 12 from Laboratory."),
        ("Laboratory", "Get the total count of RBC where T-BIL > 0.2 in Laboratory."),
        ("Laboratory", "Fetch the range of UA values where CRP = 2 in Laboratory."),
        ("Laboratory", "Retrieve the minimum value of ALP where ALP > 15 in Laboratory."),


        # Patient table
        ("Patient", "From the patient records, where sex of person is = 'M' get me his bitrthday"),
        ("Patient", "Get me the id of patients where their `first date` is < '1985-10-01' and group this by diagnosis results."),
        ("Patient", "In the ecords, find diagnosis of patients grouped by their sex.")
    ]
        elif self.db_name == 'superhero':
            self.t = [

                #superhero table
                ("superhero","Fetch superhero_name values from superhero where full_name = 'Charles Chandler'."),
                ("superhero", "Get all unique values of full_name where gender_id = 1 from superhero"),
                ("superhero", "Fetch superhero_name values where heigh_cm > 190 from superhero."),

                #race table
                

                
            ]
        elif self.db_name == 'european_football_2':
            self.t = [
                ("Leagues", "Get the total count of leagues where country_id > 10000 from Leagues."),
                ("Leagues", "Fetch unique country_ids from Leagues."),
                ("Leagues", "Fetch distinct league names where country_id = 7809 in Leagues."),
                ("Leagues", "Retrieve the id of the league where country_id = 1729 in Leagues."),

                ("Players", "Get the player_name where id = 1 from Players."),
                ("Players", "Retrieve the height of the player where player_name = 'Aaron Cresswell' in Players."),
                ("Players", "Fetch unique player_fifa_api_id values from Players."),
                ("Players", "Fetch distinct birthdays where weight = 187 in Players."),

                ("Teams", "Get the team_long_name where id = 1 from Teams."),
                ("Teams", "Retrieve the team_short_name where team_fifa_api_id = 675 in Teams."),
                ("Teams", "Fetch unique team_api_id values from Teams."),
                ("Teams", "Fetch distinct team_fifa_api_id where team_long_name = 'Beerschot AC' in Teams.")
            ]
   
    def __call__(self, t):
        print("#"*100)
        try:
            table, query = t
            print("NL Query : ", query)
            self.query = query
            cfg_string, parser, corrs = get_parser(self.schema_description, table, print_cfg=False)
            self.corrs = corrs 
            self.cfg = cfg_string
            query = restructure_query(query, corrs)
            print("Reconstructed Query : ", query)
            self.reconstructed_query = query
            tokens = tokenizer(query)
            tree = next(parser.parse(tokens))
            print("Parse Tree : ", tree)
            self.tree = tree
            sql = parse_tree_to_sql(tree)
            self.sql = sql
            print("SQL : ", sql)
            res = query_sql(f'SQL/{self.db_name}/{self.db_name}.sqlite', sql)[:self.max_sample]
            print("Results : ", res)
            return res
        except Exception as E:
            if self.debug:
                print(f"[ERROR] {E}")
            print("Results : ", [])
            return []
