
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
## (5) fetchone
## (6) disconnect
## (7) commit
## (8) insert (in cursor.execute())
## (9) update (in cursor.exsecute())
##

from MySolrDbParse import parseWhereClause
from MySolrConnect import solr_request, solr_add

class MySolrDb():
    def __init__(self, **kwargs):
        self.ignore_overwrite = kwargs.get('ignore_overwrite', False)       # do we check for doc presence before we overwrite
        self.auto_commit = kwargs.get('auto_commit', False)
        pass

    def connect(self, **kwargs):
        self.ip_address = kwargs.get('ip_address','localhost')
        self.port = kwargs.get('port', 8983)
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
            return self.processSelect(statement)

        ###########################################
        ## process insert statements here ...    ##
        ###########################################
        elif sa[0].lower().strip() == 'insert':
            print "insert"

        #########################################
        ## process update statements here ...  ##
        #########################################
        elif sa[0].lower().strip() == 'update':
            return self.processUpdate(statement)

        ############################################################
        ## else we have an unsupported format on our hands ...    ##
        ############################################################
        else:
            raise Exception, 'MySolrDb.InvalidStatement - 002'

        return None


    def handleWhereClause(self, where_clause):
        return parseWhereClause(where_clause)


    def processUpdate(self, statement):
        # currently only the following formats are supported ...
        # UPDATE table_name SET age='22' WHERE age='21
        # or
        # UPDATE table_name SET age='22', year=17, bla='bla' WHERE some condition

        # we always require a table name
        tindx = statement.lower().find('set')
        if tindx == -1:
            raise Exception, 'MySolrDb.InvalidStatement - 001'

        table_name = statement[len('update'):tindx].strip()

        # next we assemble our list of field_name/value pairs
        field_portion = ""
        # which requires us to know where the where clause starts
        # so we might as well deal with that here as well
        windx = statement.lower().find('where')
        if windx == -1:
            field_portion = statement[tindx+len('set')+1:].strip()
        else:
            field_portion = statement[tindx+len('set')+1:windx].strip()

        fields_array = field_portion.split(",")
        update_fields = {}
        for field in fields_array:
            name_val_array = field.split('=')
            name = name_val_array[0].strip()
            val = name_val_array[1].strip()
            update_fields[name] = val

        #################
        ## FIX THIS!!! ##
        #################
        table_index_field = 'id'    # BUG - this magic needs to be addressed ASAP!
        #####################
        ## END FIX THIS!!! ##
        #####################

        where_clause = ""
        if windx == -1:
            # if no where clause we're updating all of em
            where_result = table_index_field + ":[0 TO *]"
        else:
            where_portion = statement[windx:]

            # else we use standard where clause parser
            where_clause = where_portion.strip()[len('where'):].strip()
            where_result = self.handleWhereClause(where_clause)

        # we produce a solr statement to get affected docs
        solr_statement = "fl=*" + "&q=" + where_result

        # we query the solr index 
        solr_host = self.ip_address + ":" + str(self.port) 
        solr_statement = "start=0&rows=9999&" + solr_statement
        temporary_result_set = solr_request(solr_host, solr_statement)

        # next we apply the user changes to this set of documents
        for result in temporary_result_set:
            for field in update_fields:
                result[field] = update_fields[field]
            # and we also must update the solr index ...
            ret_code = solr_add(solr_host, solr_statement)


        return 0


    def processSelect(self, statement):
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





# assuming we have a solr index with a cat field and some docs
# that have electronics in that field then the following should work ...
db = MySolrDb()
db.connect(ip_address='localhost', port=8983)
## TODO: Waaa - I want db = MySolrDb.connect() like the big boys. sergey, where r u when i need u?
cursor = db.cursor()
sql_statement = "select * from joe WHERE cat = 'electronics'"
res = cursor.execute(sql_statement)
rows = cursor.fetchall()

'''
print "rows for ", sql_statement, "is --->", rows
for row in rows:
    print row
 
'''

sql_statement = "update joe set popularity = 3, manu='joe blow' WHERE cat = 'electronics'"
res = cursor.execute(sql_statement)
rows = cursor.fetchall()
'''
print "rows for ", sql_statement, "is --->", rows
for row in rows:
    print "\nROW:", row

'''



