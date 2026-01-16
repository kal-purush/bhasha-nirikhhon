from easysnmp import Session

def getinfoprinter(printer):
    session = Session(hostname=printer, community="public", version=2)
    print('Informacion de la impresora')
    print(str(session.walk('1.3.6.1.2.1.43.10.2'))) # Imprime el numero de paginas impresas
    print(str(session.walk('1.3.6.1.2.1.43.11.1.1.7'))) # Imprime los niveles de tinta
    print(str(session.walk('1.3.6.1.2.1.25.3.5.1.1'))) # Imprime el estado de la impresora