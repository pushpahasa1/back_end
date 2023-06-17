import psycopg2
import sqlite3
from fastapi import FastAPI

app = FastAPI(title = 'employee_details')

##establishing the connection
#con = psycopg2.connect(database = "sampledb",user = "pgadmin",password = "p123456",host = "127.0.0.1",port = "5432")
con = psycopg2.connect("host=127.0.0.1 port=5432 dbname=sampledb user=pgadmin password=p12345 sslmode=prefer connect_timeout=10 sslcert=<STORAGE_DIR>/.postgresql/postgresql.crt sslkey=<STORAGE_DIR>/.postgresql/postgresql.key")
con.autocommit = True

#Creating a cursor object using the cursor() method
cursor = con.cursor()

#Database creation code
def database_creation():
    #Preparing query to create a database
    sql = '''CREATE database sampledb'''

    #Creating a database
    cursor.execute(sql)
    print("Database created successfully........")

#database_creation()

#Table creation code
def table_creation():
    #Droping EMPLOYEE table if already exists.
    #cursor.execute("DROP TABLE IF EXISTS EMPLOYEE_Details")

    #Creating EMPLOYEE_Details Table
    sql ='''CREATE TABLE IF NOT EXISTS EMPLOYEE_Details(
    EmpID VARCHAR(20) NOT NULL PRIMARY KEY,    
    FIRST_NAME CHAR(20) NOT NULL,
    LAST_NAME CHAR(20),
    GENDER CHAR(1),
    PHONE_NUMBER NUMERIC,
    EmployeeEmail domain_email,
    Address VARCHAR(80),
    BloodGroup VARCHAR(2),
    EmergencyContactNumber NUMERIC
    )'''
    cursor.execute(sql)

    #Creating EMPLOYEE_asset_Details Table
    sql ='''CREATE TABLE IF NOT EXISTS ASSET_Details( 
    EmpID VARCHAR(20) NOT NULL, 
    AssetName text[] NOT NULL PRIMARY KEY,
    AssetType VARCHAR(100),
    FOREIGN KEY(EmpID) REFERENCES EMPLOYEE_Details(EmpID)
    )'''
    cursor.execute(sql)


    print("Tables created successfully........")

#table_creation()

@app.post("/emp_apis/create_employee_details/",tags=["employee_apis"])
def create_employee_details(EmpID, 
    FIRST_NAME,
    LAST_NAME,
    GENDER,
    PHONE_NUMBER,
    EmployeeEmail,
    Address,
    BloodGroup,
    EmergencyContactNumber):
    employee_list = [EmpID,  
    FIRST_NAME,
    LAST_NAME,
    GENDER,
    PHONE_NUMBER,
    EmployeeEmail,
    Address,
    BloodGroup,
    EmergencyContactNumber]
    
    sql_employee_details = """Insert into EMPLOYEE_Details (EmpID,    
    FIRST_NAME,
    LAST_NAME,
    GENDER,
    PHONE_NUMBER,
    EmployeeEmail,
    Address,
    BloodGroup,
    EmergencyContactNumber) values (%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
    
    try:
        #execute insert statement
        cursor.execute(sql_employee_details,employee_list)
        return employee_list
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error

    

@app.post("/asset_apis/create_asset_details/",tags=["asset_apis"])
def create_asset_details( EmpID,    
    AssetName,    
    AssetType):
    split_AssetName = AssetName.split(",")
    asset_list = [EmpID,split_AssetName,AssetType]

    sql_asset_details = """Insert into ASSET_Details (EmpID,AssetName,AssetType) values (%s,%s,%s);"""

    try:
        #execute insert statement
        cursor.execute(sql_asset_details,asset_list)
        return asset_list
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error

@app.put("/emp_apis/update_employee_details/",tags=["employee_apis"])
def update_employee_details( EmpID, 
    FIRST_NAME = None,
    LAST_NAME = None,
    GENDER = None,
    PHONE_NUMBER = None,
    EmployeeEmail = None,
    Address = None,
    BloodGroup = None,
    EmergencyContactNumber = None) :
    update_employee_list = [
    FIRST_NAME,
    LAST_NAME,
    GENDER,
    PHONE_NUMBER,
    EmployeeEmail,
    Address,
    BloodGroup,
    EmergencyContactNumber]
    update_employee_list_str = [
    "first_name",
    "last_name",
    "gender",
    "phone_number",
    "employeeemail",
    "address",
    "bloodgroup",
    "emergencycontactnumber"]
  
    try:
        #execute update statement
        for index in range(len(update_employee_list)):
            if update_employee_list[index] != None:
                str_atr = update_employee_list_str[index]
                sql="UPDATE EMPLOYEE_Details SET " + str_atr + " = %s where EmpID = %s;"
                exe_sql = [update_employee_list[index],EmpID]
                cursor.execute(sql,exe_sql)
        return update_employee_list
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error


@app.put("/asset_apis/update_asset_details/",tags=["asset_apis"])
def update_asset_details( EmpID,    
    AssetName = None,    
    AssetType = None):
    split_AssetName = AssetName.split(",")
    update_asset_list = [split_AssetName,AssetType]

    update_asset_list_str = [
    "assetname",
    "assettype",]
  
    try:
        #execute update statement
        for index in range(len(update_asset_list)):
            if update_asset_list[index] != None:
                str_atr = update_asset_list_str[index]
                sql="UPDATE ASSET_Details SET " + str_atr + " = %s where EmpID = %s;"
                exe_sql = [update_asset_list[index],EmpID]
                cursor.execute(sql,exe_sql)
        return update_asset_list

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error

@app.get("/emp_apis/read_employee_details/",tags=["employee_apis"])
def read_employee_details():
    try:
        # execute the read  statement
        sql = "SELECT * FROM employee_details"
        cursor.execute(sql)
        output = cursor.fetchall()
        return output
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error

@app.get("/asset_apis/read_asset_details/",tags=["asset_apis"])
def read_asset_details():
    try:
        # execute the read  statement
        sql = "SELECT * FROM asset_details"
        cursor.execute(sql)
        output = cursor.fetchall()
        return output
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error

@app.delete("/emp_apis/delete_employee_details/",tags=["employee_apis"])
def delete_employee_details(EmpID):
    asset_rows_deleted = 0
    employee_rows_deleted = 0
    try:
        # execute the Delete  statement
        cursor.execute("DELETE FROM ASSET_Details WHERE EmpID = %s", (EmpID,))
        cursor.execute("DELETE FROM EMPLOYEE_Details WHERE EmpID = %s", (EmpID,))

        # get the number of updated rows
        asset_rows_deleted = cursor.rowcount
        # get the number of updated rows
        employee_rows_deleted = cursor.rowcount
        return [asset_rows_deleted,employee_rows_deleted]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error

@app.delete("/asset_apis/delete_asset_details/",tags=["asset_apis"])
def delete_asset_details(EmpID):
    asset_rows_deleted = 0
    try:
        # execute the Delete  statement
        cursor.execute("DELETE FROM ASSET_Details WHERE EmpID = %s", (EmpID,))

        # get the number of updated rows
        asset_rows_deleted = cursor.rowcount
        return asset_rows_deleted
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error

column_names = [ "empid",
    "first_name",
    "last_name",
    "gender",
    "phone_number",
    "employeeemail",
    "address",
    "bloodgroup",
    "emergencycontactnumber",
    "empid",
    "assetnames",
    "asset_count"]
@app.get("/read_all_details/",tags=["mapping_apis"])
def read_employee_details():
    try:
        # execute the read  statement
        sql = """select * from (SELECT * FROM employee_details) as emp_table inner join 
        (SELECT empid,assetname from asset_details) 
        as asset_table on emp_table.empid = asset_table.empid"""
        #sql = "SELECT empid,assetname,count(DISTINCT assetname) from asset_details as asset group by empid,assetname"
        cursor.execute(sql)
        output = cursor.fetchall()
        output_dictionary = {"EmployeeList":[]}
        for each_output in output:
            asset_count = tuple(str(len(list(each_output)[-1])))
            output_dictionary["EmployeeList"].append(dict(zip(column_names, each_output+asset_count)))

        #save output
        file = open ("output.json","w")
        file.write(str(output_dictionary))
        file.close()
        
        return output_dictionary
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return error   
