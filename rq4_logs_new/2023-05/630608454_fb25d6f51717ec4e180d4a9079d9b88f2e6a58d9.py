import pyodbc
class Crimen:
    def __init__(self):
        server= 'DESKTOP-TL8EMPL'
        bd='SSPP'
        usuario='sa'
        contrasena='080322'
        self.cnn=pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server+' ;DATABASE='+bd+';UID='+usuario+';PWD='+contrasena)

    def __str__(self):
        datos=self.consulta_crimen()        
        aux=""
        for row in datos:#contatenamos
             aux=aux + str(row) + "\n"
        return aux

    def consulta_crimen(self):
        cur = self.cnn.cursor()
        cur.execute("SELECT Crimen.Nombre from Crimen")
        datos = cur.fetchall()
        cur.close()    
        return datos