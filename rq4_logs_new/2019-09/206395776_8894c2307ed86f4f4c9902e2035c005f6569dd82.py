
#creates SQL query for hamming distance 1, including exact matches
def hamSQL(s):
    SQL = "SELECT * FROM VIRUSES WHERE "
    for index in range(0, len(s)):
        SQL +=  "SEQUENCE LIKE '%" + s[:index]+"_"+s[index+1:] + "%'"
        if index != len(s) -1:
            SQL += " OR "
    return SQL

def exactSQL(s):
    #identical to:
    # Virus.query.filter(Virus.sequence.like('%' + searchSeq + '%')).all()
    SQL = "SELECT * FROM VIRUSES WHERE SEQUENCE LIKE '%{}%'".format(s)
    return SQL