
##
## MySolrDbParse.py - primitive where clause converter 
## for MySolrDb.py
##
import cStringIO, tokenize

def atom(token):
    if token[0] is tokenize.NAME:
        return token[1]
    elif token[0] is tokenize.STRING:
        return token[1][1:-1].decode("string-escape")
        #return "'" + token[1][1:-1].decode("string-escape") + "'"
    elif token[0] is tokenize.NUMBER:
        try:
            return int(token[1], 0)
        except ValueError:
            return float(token[1])
    elif token[0] is tokenize.OP:
        return token[1]
    else:
        return None

def parseWhereClause(source):
    src = cStringIO.StringIO(source).readline
    src = tokenize.generate_tokens(src)
    operator = ""
    state = 0   # 0 = gathering first parameter
    solr_statement = ""
    for s in src:
        if state == 0:
            # state = 0 = gathering first parameter
            state += 1
            sub_statement = ""
            if s[0] is tokenize.NAME:
                sub_statement = s[1]+":"
                p1_type = 'column'
            elif s[0] is tokenize.STRING or s[0] is tokenize.NUMBER:
                sub_statement = ":"+str(atom(s))
                p1_type = 'constant'
            else:
                raise Exception, 'Ill formed statement'
            
        elif state == 1:
            state += 1
            # state = 1 = gathering operator
            if s[0] is tokenize.OP:
                operator = s[1]
            else:
                raise Exception, 'Invalid operator'

        elif state == 2:
            state += 1
            # state = 2 = gathering second parameter
            if s[0] is tokenize.NAME:
                if p1_type == 'column':
                    sub_statement += s[1]
                else:
                    raise Exception, 'Column to Column compare currently unsupported!'
            elif s[0] is tokenize.STRING or s[0] is tokenize.NUMBER:
                if p1_type == 'column':
                    sub_statement += str(atom(s))
                else:
                    raise Exception, 'Error - constant to constant compare detected!'
            else:
                raise Exception, 'Ill formed statement'

        elif state == 3:
            state += 1
            if s[0] is tokenize.ENDMARKER:
                return solr_statement + produceSolrOutput(sub_statement, operator)
            else:
                if s[0] is tokenize.NAME and s[1].lower() == 'and':
                    #print "Disjunction!"
                    state = 0
                    solr_statement = solr_statement + produceSolrOutput(sub_statement, operator) + " AND "
                elif s[0] is tokenize.NAME and s[1].lower() == 'or':
                    #print "Conjunction!"
                    state = 0
                    solr_statement = solr_statement + produceSolrOutput(sub_statement, operator) + " OR "
                else:
                    raise Exception, 'Missing EndMarker'

        else:
            print "Creepy Internal Error 1001", state, s


def produceSolrOutput(sub_statement, operator):
    if operator == '=':
        return sub_statement
    elif operator == '!=':
        return "-"+sub_statement
    elif operator == '>':
        a = sub_statement.split(":")
        return a[0] + ":[" + a[1] + " TO *]"
    elif operator == '<':
        a = sub_statement.split(":")
        return a[0] + ":[* TO " + a[1] + "]"
    elif operator == '>=':
        a = sub_statement.split(":")
        return a[0] + ":[" + a[1] + " TO *]"
    elif operator == '<=':
        a = sub_statement.split(":")
        return a[0] + ":[* TO " + a[1] + "]"
    else:
        raise Exception, 'Error - Unsupported operator - '+operator


################
## unit tests ##
################
'''
statement = "x > 75"
res = parseWhereClause(statement)
print "statement:", statement, "returned --->", res


statement = "x > 75 and x < 80"
res = parseWhereClause(statement)
print "statement:", statement, "returned --->", res


statement = "field_name = '75'"
res = parseWhereClause(statement)
print "statement:", statement, "returned --->", res


statement = "field_name >= 'xYz'"
res = parseWhereClause(statement)
print "statement:", statement, "returned --->", res

statement = "user_name = 'Joe' AND user_age > 75"
res = parseWhereClause(statement)
print "statement:", statement, "returned --->", res

'''

