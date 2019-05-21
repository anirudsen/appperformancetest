import pyodbc
import collections
import json
import datetime
import time
import calendar
from flask import Flask, request,  jsonify
from sqlalchemy import create_engine
import snowflake.connector


app = Flask(__name__)

@app.route('/')
def getwelcomeMsg():
    ctx = snowflake.connector.connect(
    user='aniruddha.sen',
    password='Candy2019g@od',
    account='xerox',
	region='east-us-2.azure',
	warehouse='TESTBILLINGWH',
	database='test',
	schema='test',
	role='SYSADMIN'
    )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
        ctx.close()
    return 'Asset API with /'



@app.route('/v1/performancetest',methods=['POST'])
def getData():
    startTime = str(datetime.datetime.now())

    '''
    mssql_host = 'tcp:mdpsqldbserverdev.database.windows.net'
    mssql_db = 'mdpappdb'
    mssql_user = 'mdpadmin'
    mssql_pwd = 'Robo#2010'
    mssql_port = 1433 
    mssql_driver = 'ODBC Driver 17 for SQL Server'
    database_server_name = "mdpsqldbserverdev"
    dns = 'testodbc'
    '''
    mssql_host = 'tcp:mdpsqldbserverqc.database.windows.net'
    mssql_db = 'mdp'
    mssql_user = 'appadmin'
    mssql_pwd = 'Robo#2010'
    mssql_port = 1433 
    mssql_driver = 'ODBC Driver 17 for SQL Server'
    database_server_name = "mdpsqldbserverdev"
    dns = 'testodbc'
    #-----------------------------------------------------
    content=request.get_json()
    #tablename=content['tableName']
    #columnname=content['columnName']
    filtercondition=content['Full']
    incrementaldate=content['incrementalDate']
    offset=content['offSet']
    limit=content['Limit']
    
    #-----------------------------------------------------
    
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+mssql_host+';DATABASE='+mssql_db+';UID='+mssql_user+';PWD='+ mssql_pwd+';Trusted_connection=no')
    cursor = cnxn.cursor()
    sql_query = " "
    if filtercondition == '*'  and incrementaldate != ' ' :
        sql_query = "SELECT * FROM (SELECT *, Row_number() OVER (ORDER BY Asset_Identifier DESC) AS rownum FROM [dbo].iSolve_Asset_Stg)tb1 WHERE rownum between " + offset + "AND " + limit + " AND Last_Update_Date >='"+incrementaldate+"';"
        print(sql_query)
        #sql_query = "SELECT * FROM "+ "dbo.iSolve_Asset_Stg" +" WHERE Last_Update_Date >='" + strDate + "';"
    if filtercondition == '*'  and incrementaldate == ' ' :
        sql_query = "SELECT * FROM (SELECT * , Row_number() OVER (ORDER BY Asset_Identifier DESC) AS rownum FROM [dbo].iSolve_Asset_Stg)tbl WHERE rownum between " + offset + "AND " + limit +";"
        #sql_query = "SELECT * from "+"dbo.iSolve_Asset_Stg ORDER BY ASSET_Identifier" +"OFFSET "+ offset + "FETCH NEXT "+limit+" ROWS ONLY"+";"

 
    cursor.execute(sql_query) 
    #rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    results = []
    
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    endTime = str(datetime.datetime.now())
    #rec = [ dict(rec) for rec in rows ]
    results.append("startTime: "+startTime)
    results.append("endTime: "+endTime)

#j = json.dumps(objects_list,myconverter)
    return jsonify(results)


    