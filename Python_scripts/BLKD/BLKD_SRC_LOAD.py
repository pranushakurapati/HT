
import sys
sys.path.append(".")

import snowflake.connector
import json
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

config_engine = {
    'user':'ABServiceAccountSnowflake', #'ABServiceAccountSnowflake',
    'password': 'Hightower2019!',
    'account': 'hightoweradvisors.us-east-1', #'saggezzapartner.us-east-1',
    'warehouse': 'demo_wh',#'HT_WH',# Needs to be changed accordingly with respect to High Tower Instance
    'database': 'HT_SOURCE_DB',# Needs to be changed accordingly with respect to High Tower Instance
    'schema': 'CONFIG',# Needs to be changed accordingly with respect to High Tower Instance
    'role': 'DEVELOPER', #'HT_DEVELOPER',
    'numpy': True
}

def create_connection(engine_name):
    engine = create_engine(URL(
                                user=engine_name['user'],
                                password=engine_name['password'],
                                account=engine_name['account'],
                                warehouse=engine_name['warehouse'],
                                database=engine_name['database'],
                                schema=engine_name['schema'],
                                role= engine_name['role'],
                                numpy=engine_name['numpy']
                                ))
    return engine.connect()


config_connection = create_connection(config_engine)


stage_connection = json.loads(config_connection.execute('''select connection_str from connectiondata 
                                              where connection_id=1''').fetchone()[0])
                                              
                                              
source_connection = json.loads(config_connection.execute('''select connection_str from connectiondata 
                                              where connection_id=2''').fetchone()[0])

stg = create_connection(stage_connection)
src = create_connection(source_connection)


query = '''insert into BLKD_ACCOUNT_IMP(
select 
Data:AccountCategory::varchar as AccountCategory
,Data:AccountNumber::varchar as AccountNumber
,Data:AccountSubCategory::varchar as AccountSubCategory
,Data:AccountType::varchar as AccountType
,Data:Address::varchar as Address
,Data:AsOfDate::varchar as AsOfDate
,Data:Billable::varchar as Billable
,Data:BillingNumber::varchar as BillingNumber
,Data:ClosedDate::varchar as ClosedDate
,Data:Custodian::varchar as Custodian
,Data:DataProvider::varchar as DataProvider
,Data:Discretionary::varchar as Discretionary
,Data:Id::varchar as Id
,Data:LastReconciledDate::varchar as LastReconciledDate
,Data:LongNumber::varchar as LongNumber
,Data:Name::varchar as Name
,Data:StartDate::varchar as StartDate
,Data:Supervised::varchar as Supervised
,Data:Targets::varchar as Targets
,Data:TaxMethodology::varchar as TaxMethodology
,Data:TaxStatus::varchar as TaxStatus
from HT_STAGE_DB.VENDOR.BLKD_ACCOUNT_STG)'''


insert_op = src.execute(query)
print(insert_op.fetchall())


stg.close()
src.close()

