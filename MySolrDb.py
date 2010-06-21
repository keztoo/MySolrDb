
##
## MySolrDb.py - plug-in replacement for MySqlDb
## python library to support SQL syntax using 
## a solr index as the database.
##
## MIA
## (1) drop database
## (2) drop table
## (3) fetchone
## (4) disconnect
## (5) commit
## (6) delete
##

from MySolrDbParse import parseWhereClause
from MySolrConnect import solr_request, solr_add, solr_commit


class MySolrDb():
    def __init__(self, **kwargs):
        self.ignore_overwrite = kwargs.get('ignore_overwrite', False)       # do we check for doc presence before we overwrite
        self.auto_commit = kwargs.get('auto_commit', False)
        pass

    def connect(self, **kwargs):
        self.ip_address = kwargs.get('ip_address','localhost')
        self.port = kwargs.get('port', 8983)
        # TODO: we should probably pull in all tables here and save them 
        return self

    def cursor(self, **kwargs):
        return MySolrDbCursor(ip_address=self.ip_address, port=self.port)


class MySolrDbCursor():
    def __init__(self, **kwargs):
        self.ip_address = kwargs.get('ip_address', None)
        self.port = kwargs.get('port', None)
        self.auto_commit = kwargs.get('auto_commit', False)
        self.last_result = None
        self.database_name = None

    def fetchall(self):
        return self.last_result

    def execute(self, statement):
        # for now we only support 'select', 'insert', 'update', 
        # 'delete', 'create database' and 'create table'
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
            return self.processInsert(statement)

        #########################################
        ## process update statements here ...  ##
        #########################################
        elif sa[0].lower().strip() == 'update':
            return self.processUpdate(statement)

        ######################################
        ## process use statements here ...  ##
        ######################################
        elif sa[0].lower().strip() == 'use':
            return self.processUse(statement)

        #########################################
        ## process delete statements here ...  ##
        #########################################
        elif sa[0].lower().strip() == 'delete':
            return self.processDelete(statement)

        #########################################
        ## process create statements here ...  ##
        #########################################
        elif sa[0].lower().strip() == 'create':
            # for create we have database and tables ...
            tmp = statement.split(" ")
            if len(tmp) < 3:
                raise Exception, 'Illegal statement!'
            if tmp[1].lower().strip() == 'database':
                return self.createDatabase(statement)
            elif tmp[1].lower().strip() == 'table':
                return self.createTable(self.database_name, statement)
            else:
                raise Exception, 'Illegal statement!'
            
        ############################################################
        ## else we have an unsupported format on our hands ...    ##
        ############################################################
        else:
            raise Exception, 'MySolrDb.InvalidStatement - 002'

        return None


    def handleWhereClause(self, where_clause):
        return parseWhereClause(where_clause)



    def processUse(self, statement):
        database_name = statement[len('use'):].strip()
        # someday we will validate this 
        self.database_name = database_name
        return 0


    def processDelete(self, statement):
        pass


    def processInsert(self, statement):
        # currently only the following formats are supported ...
        # INSERT INTO tablename (field1, field2) VALUES (field1_val, field2_val)
        # insert has the additional problem of having to support auto-increment fields

        iindx = statement.lower().find('into')
        if iindx == -1:
            raise Exception, 'MySolrDb.InvalidStatement - 001'

        # we always require a table name which is followed by '('
        tindx = statement.lower().find('(')
        if tindx == -1:
            raise Exception, 'MySolrDb.InvalidStatement - 001'

        table_name = statement[len('insert into'):tindx].strip()

        # next determine database name
        if table_name.find(".") > -1:
            # table name includes the database name
            database_name, table_name = table_name.split(".")
        else:
            # else must have a default db name from a previous use or connect
            if self.database_name is None:
                raise Exception, 'Error - no database specified'
            database_name = self.database_name


        # next we extract the field names TODO: validate them
        fn_start = tindx + 1
        fn_end = statement.find(")")
        if fn_end == -1:
            raise Exception, 'MySolrDb.InvalidStatement - 001'

        fields = statement[fn_start:fn_end]

        # we next want our values
        vals_start = statement.find("(", fn_end)
        if vals_start == -1:
            raise Exception, 'MySolrDb.InvalidStatement - 001'

        vals_end = statement.find(")", vals_start)
        if vals_end == -1:
            raise Exception, 'MySolrDb.InvalidStatement - 001'

        vals = statement[vals_start+1:vals_end]

        fields_array = fields.split(",")
        vals_array = vals.split(",")

        insert_transaction = "<add><doc><field name='id'>%s</field>" % (self.getNextId(),)
        indx = 0
        # TODO: handle array fields
        while indx < len(fields_array):
            #insert_transaction += "<field name='%s_%s_%s'>%s</field>" % (database_name, table_name, fields_array[indx].strip(), vals_array[indx].strip())
            column_name = fields_array[indx].strip()
            column_val = vals_array[indx].strip()
            insert_transaction += "<field name='column_name'>%s</field><field name='column_type'>%s</field>" % (database_name + "_" + table_name + "_" + column_name, 'string')
            insert_transaction += "<field name='column_val_string'>%s</field>" % (column_val,)
            indx += 1
        insert_transaction += "</doc></add>"

        # finally we send this to solr
        res = solr_add(self.ip_address+":"+str(self.port), insert_transaction)
        res = solr_commit("localhost:8983")
        return res


    def processUpdate(self, statement):
        # currently only the following formats are supported ...
        # UPDATE table_name SET age='22' WHERE some condition
        # or
        # UPDATE table_name SET age='22', year=17, bla='bla' WHERE some condition

        # we always require a table name which is followed by 'set'
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

        # at this point, update_fields is a dict of fields/values which need to
        # be updated for each doc which meets the where_clause critera

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
            where_clause = where_portion.strip()[len('where'):].strip()
            # else we use our standard where clause parser
            where_result = self.handleWhereClause(where_clause)

        # we produce a solr statement to get affected docs
        solr_statement = "fl=*" + "&q=" + where_result

        # we query the solr index and stick the result in temporary_result_set
        solr_host = self.ip_address + ":" + str(self.port) 
        solr_statement = "start=0&rows=9999&" + solr_statement
        temporary_result_set = solr_request(solr_host, solr_statement)

        # next we apply the user changes to this set of documents
        # and produce a solr style xml doc which represents a transaction
        update_transaction = "<add>"
        for result in temporary_result_set:
            for field in update_fields:
                result[field] = update_fields[field]
            # and we also must update the solr index ...
            # note - for transactional integrity we really should'nt
            # update individually but rather we create a
            # master update which we finish with a commit below
            #ret_code = solr_add(solr_host, result)
            update_transaction += self.convertDictToDoc(result)
        update_transaction += "</add><commit/>"

        # finally we update our solr index
        res = solr_add(self.ip_address+":"+str(self.port), update_transaction)
        print "res --->", res

        # and return the update status from the solr response
        return 0


    def convertDictToDoc(self, dict):
        # convert a dict to a string of solr name/value entries
        # only real work to be done is to handle array fields
        doc = "<doc>"
        for key in dict:
            if isinstance(dict[key], list):
                for k in dict[key]:
                    doc += "<field name='%s'>%s</field>" % (key, k)
            else:
                doc += "<field name='%s'>%s</field>" % (key, dict[key])
        doc += "</doc>"
        return doc


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

    def createDatabase(self, statement):
        # expect 'create database database_name"
        dindx = statement.find("database")
        if dindx == -1:
            raise Exception, 'Error - illegal statement'
        dindx += len('database')
        database_name = statement[dindx:].strip()
        # first we check to see if we already have a database with the same name
        solr_str = "fl=id&q=database_name:" + database_name 
        database_record = solr_request("localhost:8983", solr_str)
        if len(database_record) > 0:
            raise Exception, 'Error database already exists!'

        solr_str = "<add><doc><field name='id'>%s</field><field name='database_name'>%s</field></doc></add>" % (self.getNextId(),database_name)
        res = solr_add("localhost:8983", solr_str)
        res = solr_commit("localhost:8983")
        return res


    def getNextId(self):
        solr_str = "fl=id&q=id:[0 TO *]&sort=id desc&start=0&rows=1"
        last_id = solr_request("localhost:8983", solr_str)
        try:
            last_id = last_id[0]['id']
        except:
            print "Warning - last id was -1, adjusting to 1"
            return 1
        return int(last_id + 1)


    def createTable(self, database_name, statement):
        # supported format ...
        # CREATE TABLE test_table (id int, name VARCHAR(32), ssn VARCHAR(32));

        # note we have an issue to deal with here. if our table name is of the 
        # form x.table_name then we need to use the database name (x) from the 
        # create statement otherwise we fall back to our self.database_name
        # field and if both are None then we must throw an exception
        # TODO: for now i hard code it to 'mydb'

        #database_name = 'mydb'

        # first, extract table name
        tindx = statement.lower().find('table')
        if tindx == -1:
            raise Exception, 'Error ill formed statement'
        tindx += len('table')

        pindx = statement.find("(")
        if pindx == -1:
            raise Exception, 'Error ill formed statement'

        #table_name = database_name + "_" + statement[tindx:pindx].strip()

        table_name = statement[tindx:pindx].strip()
        # next determine database name
        if table_name.find(".") > -1:
            # table name includes the database name
            database_name, table_name = table_name.split(".")
        else:
            # else must have a default db name from a previous use or connect
            if self.database_name is None:
                raise Exception, 'Error - no database specified'
            database_name = self.database_name

        # and for our purposes table name is databasename skid tablename
        table_name = database_name + "_" + table_name

        # NOTE - we have a serious issue with ids !!!! until then we will commit after every update
        solr_transaction = "<add><doc><field name='id'>%s</field><field name='table_name'>%s</field></doc></add>" % (self.getNextId(), table_name)
        res = solr_add("localhost:8983", solr_transaction)
        res = solr_commit("localhost:8983")

        table_name += "_"

        columns = statement[pindx+1:]
        columns = columns[:-1]

        column_array = columns.split(",")
        for column in column_array:
            # TODO: support default values !!! and other attributes (like enums!)
            solr_str = ""
            ca = column.split()
            column_name = ca[0].strip()

            column_type = ca[1].strip().lower()
            if column_type[:len('int')] == 'int':
                column_type = 'int'
            elif column_type[:len('varchar')] == 'varchar':
                column_type = 'string'
            elif column_type[:len('float')] == 'float':
                column_type = 'float'
            elif column_type[:len('timestamp')] == 'timestamp':
                column_type = 'date'
            else:
                raise Exception, "Error invalid column type"

            column_attribute1 = ""
            column_default_val = 0
            if len(ca) > 2:
                column_attribute1 = ca[2].strip()

            solr_str = "<field name='id'>%s</field><field name='meta_column_name'>%s</field><field name='meta_column_type'>%s</field>" % (self.getNextId(), table_name + column_name, column_type)

            if column_type == 'int':
                solr_str += "<field name='meta_column_val_int'>%s</field>" % (column_default_val,)
            elif column_type == 'string':
                solr_str += "<field name='meta_column_val_string'>%s</field>" % (column_default_val,)
            elif column_type == 'float':
                solr_str += "<field name='meta_column_val_float'>%s</field>" % (column_default__val,)
            elif column_type == 'date':
                solr_str += "<field name='meta_column_val_date'>%s</field>" % (column_default_val,)
            else:
                raise Exception, "Error invalid column type"

            solr_str = "<add><doc>" + solr_str + "</doc></add>"
            res = solr_add("localhost:8983", solr_str)
            res = solr_commit("localhost:8983")
        return res



####################################################
## eventually move this out to the unit test file ##
####################################################

db = MySolrDb()
db.connect(ip_address='localhost', port=8983)
## TODO: Waaa - I want db = MySolrDb.connect() like the big boys. sergey, where r u when i need u?
cursor = db.cursor()

res = cursor.execute("create database mydb")
res = cursor.execute("use mydb")
create_table_statement = "CREATE TABLE test_table (id int, name VARCHAR(32), ssn VARCHAR(32))"
res = cursor.execute(create_table_statement)

############################################
# this is like a show databases command    #
############################################
solr_str = "fl=id,database_name&q=database_name:[0 TO *]"
databases = solr_request("localhost:8983", solr_str)
print "Database on this instance --->", databases

##########################################
# for each database lets dump the tables #
##########################################
for database in databases:
    database_name = database['database_name']
    database_name = database_name[0]
    solr_str = "fl=id,table_name&q=table_name:" + database_name + "_*"
    tables = solr_request("localhost:8983", solr_str)
    print database_name, "tables --->", tables
    for table in tables:
        table_name = table['table_name']
        table_name = table_name[0]
        solr_str = "fl=id,meta_column_name,meta_column_type,meta_column_val_string,meta_column_val_int,meta_column_val_float,meta_column_val_timestamp&q=meta_column_name:" + table_name + "_*"
        columns = solr_request("localhost:8983", solr_str)
        for column in columns:
            column_name = column['meta_column_name']
            column_name = column_name[0]
            column_name = column_name.replace(table_name, "")
            column_name = column_name[1:]

            column_type = column['meta_column_type']
            column_type = column_type[0]

            column_val_name = "meta_column_val_%s" % (column_type, )
            print table_name, column_type, "column --->", column_name, "value --->", column[column_val_name]

res = cursor.execute("insert into mydb.test_table (name, ssn) values ('Joe Blow', '123-456-7890')")
print "res from insert", res    

res = cursor.execute("insert into mydb.test_table (name, ssn) values ('ken smith', '000-000-0000')")
print "res from insert", res    

# this is like a select * 
solr_str = "fl=*&q=column_name:mydb_test_table_*"
res = solr_request("localhost:8983", solr_str)
print "res from select * --->", res





