import mysql.connector

def Check_Cridentials(mycursor, Emp_Num, passW):
    Emp_Num = str(Emp_Num)
    sqlvar = "SELECT CRIDENCIALS FROM EMPLOYEES  \
                WHERE EMP_NUM =" + Emp_Num + ";"
                             
    return passW == mycursor.execute(sqlvar)
    