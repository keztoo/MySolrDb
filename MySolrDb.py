
##
## MySolrDb.py - plug-in replacement for MySqlDb
## python library to support SQL syntax using 
## a solr index as the database.
##
## MIA
## (1) create database
## (2) drop database
## (3) create table
## (4) drop table
## (5) fetchall
## (6) fetchone
## (7) disconnect
## (8) commit
## (9) insert (in cursor.execute())
## (10) update (in cursor.exsecute())
##

from MySolrDbParse import parseWhereClause
from MySolrConnect import solr_request

class MySolrDb():
    def __init__(self, **kwargs):
        self.ignore_overwrite = kwargs.get('ignore_overwrite', False)       # do we check for doc presence before we overwrite
        self.auto_commit = kwargs.get('auto_commit', False)
        pass

    def connect(self, **kwargs):
        self.ip_address = 'localhost'
        self.port = 8983
        return self

    def cursor(self, **kwargs):
        return MySolrDbCursor(ip_address=self.ip_address, port=self.port)


class MySolrDbCursor():
    def __init__(self, **kwargs):
        self.ip_address = kwargs.get('ip_address', None)
        self.port = kwargs.get('port', None)
        self.auto_commit = kwargs.get('auto_commit', False)
        self.last_result = None

    def fetchall(self):
        return self.last_result

    def execute(self, statement):
        # for now we only support select, insert, update
        # will deal with joins later (maybe)
        if statement is None:
            return statement

        # we should really use a parser here but for now
        # we will do it manually.
        sa = statement.strip().split(" ")

        ##########################################
        ## process select statements here ...   ##
        ##########################################
        if sa[0].lower().strip() == 'select':
            # we require 'from' and 'where' to also be present
            findx = statement.lower().find('from')
            windx = statement.lower().find('where')
            if findx == -1 or windx == -1:
                raise Exception, 'MySolrDb.InvalidStatement - 001'

            # since we now know where from and where are we can
            # create all three sub components 
            select_portion = statement[:findx]
            table_portion = statement[findx:windx]
            where_portion = statement[windx:]

            # we will need to process the table list first to establish 
            # any aliases we will need later in field list processing
            # NOTE: for now since we dont support joins we assume 1 table
            # and oh, btw we currently don't support aliasing
            table_name = table_portion.strip()[len('from'):].strip()
            #print "Table Name --->", table_name

            # for the select portion we knmow that ultimately
            # field names must be separated by a comma.
            field_list = select_portion.strip()[len('select'):].strip()
            solr_field_list = field_list
            solr_field_list = solr_field_list.replace(" ", "")
            field_list = field_list.split(",")
            for field in field_list:
                ## TODO: if any of these are invalid throw an exception here!
                #print "Must Validate Field Name --->", field.strip()
                pass

            # for the where portion, again we don't currently worry
            # about things like table name or aliases. 
            # since this will get ugly in a hurry we will farm it out ...
            where_clause = where_portion.strip()[len('where'):].strip()
            where_result = self.handleWhereClause(where_clause)

            # finally we can produce a solr statement
            solr_statement = "fl=" + solr_field_list + "&q=" + where_result

            # finally we query the solr index and store the results
            solr_host = self.ip_address + ":" + str(self.port) 
            solr_statement = "start=0&rows=9999&" + solr_statement
            self.last_result = solr_request(solr_host, solr_statement)

            # this should come from the response
            solr_response = "200"
            return solr_response

        ###########################################
        ## process insert statements here ...    ##
        ###########################################
        elif sa[0].lower().strip() == 'insert':
            print "insert"

        #########################################
        ## process update statements here ...  ##
        #########################################
        elif sa[0].lower().strip() == 'update':
            print "update"

        ############################################################
        ## else we have an unsupported format on our hands ...    ##
        ############################################################
        else:
            raise Exception, 'MySolrDb.InvalidStatement - 002'

        return None


    def handleWhereClause(self, where_clause):
        return parseWhereClause(where_clause)




db = MySolrDb()
db.connect()
## TODO: Waaa - I want db = MySolrDb.connect() like the big boys
cursor = db.cursor()
sql_statement = "select * from joe WHERE cat = 'electronics'"
res = cursor.execute(sql_statement)
rows = cursor.fetchall()
print "rows for ", sql_statement, "is --->", rows
for row in rows:
    print row
 

'''
sql_statement = " select id,name, age from joe WHERE bla = 'bla' and x > 7"
res = cursor.execute(sql_statement)
print "res for ", sql_statement, "is --->", res
'''



