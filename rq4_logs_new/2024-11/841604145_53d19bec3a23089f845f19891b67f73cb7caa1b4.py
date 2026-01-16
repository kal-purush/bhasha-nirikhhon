from Compuesto import Compuesto
from Estequiometria import TipoDato
from Estequiometria.Gas import Gas
from Estequiometria.problemaEsteq import problemaEsteq
from Estequiometria.Conversiones import Conversiones
from Estequiometria.Masa import Masa
from Estequiometria.Moles import Moles
from Estequiometria.Solucion import Solucion
from Estequiometria.Liquido import Liquido
from Reaccion import Reaccion

def pruebaSolucion():
    solucion1 = Solucion(compuesto= "NaOH", dimensional="mL", magnitud= 16.345, molaridad=1.501)
    print(solucion1)
    moles = solucion1.aMoles()
    print(moles)
    solucion2 = Solucion(dimensional="mL",compuesto="NaOH", magnitud=25.13)
    solucion2.getIncognita(moles)
    print(solucion2)

def pruebaMasa():
    masa = Masa(compuesto= "CuSO4", dimensional="oz", magnitud= 11.45)
    print(masa)
    moles = masa.aMoles()
    print(moles)
    masa2 = Masa(compuesto= "CuSO4", dimensional="lb")
    masa2.getIncognita(moles)
    print(masa2)
    
def pruebaLiquido():
    dato = Liquido(compuesto="Hg", magnitud=25.77, dimMagnitud="mL")
    print(dato)
    
    incognita = Liquido(compuesto="Hg", dimMagnitud="mL")
    moles = dato.aMoles()
    incognita.getIncognita(moles)
    print(incognita)
    
    dato2 = Liquido(compuesto="C6H6", magnitud = 30.56, dimMagnitud="cm3", densidad = 876, dimDensidad= "kg/m3")
    print(dato2)
    masa = dato2.aMasa()
    print(masa)
    print(dato2.aMoles())
    incognita2 = Liquido(compuesto="C6H6", magnitud = 30.56, dimMagnitud="cm3",dimDensidad="g/L")
    incognita2.getIncognita()
    print(incognita2)
    
def pruebaMoles():
    dato1 = Moles(compuesto="H2O", magnitud= 0.5)
    reaccion = "H2 + O2 = H2O"
    print(dato1.aMolesDe(reaccion,"O2"))

def pruebaReaccion():
    reaccion = Reaccion("H2 + O2 = H2O")
    compuesto = reaccion.getCompuesto(formula="O2")
    print(compuesto.getCompuesto())


#pruebaMasa()
#pruebaSolucion()
#pruebaLiquido()
#pruebaMoles()

def prueba1():
    reaccion = "H3PO4 + Ca(OH)2 = Ca3(PO4)2 + H2O"
    dato = Solucion(compuesto = "Ca(OH)2",  magnitud = 58.0, dimensional = "mL", molaridad=0.500)
    incognita = Masa(compuesto = "Ca3(PO4)2",  dimensional = "oz")
    problema = problemaEsteq([dato], incognita, reaccion)
    print(problema.deCosaACosa())

def prueba2():
    reaccion = "Cu + HNO3 = Cu(NO3)2 + H2O + NO"
    dato = Masa(compuesto ="Cu", magnitud=15.8)
    incognita = Solucion(compuesto="HNO3", dimensional="mL",molaridad=4.75)
    problema = problemaEsteq([dato], incognita, reaccion)
    print(problema.deCosaACosa())
    
def prueba3():
    reaccion = "Tl2(SO4)3 + KI = TlI3 + K2SO4"
    dato = Masa(compuesto="TlI3",magnitud=0.1964)
    incognita = Masa(compuesto="Tl2(SO4)3")
    problema = problemaEsteq([dato],incognita,reaccion)
    print(problema.deCosaACosa())
    
def prueba4():
    reaccion = "C8H18 + O2 = CO2 + H2O"
    dato = Liquido(compuesto="C8H18",dimMagnitud="mL", magnitud=3.0,dimDensidad="kg/m3",densidad=703)
    incognita = Gas(compuesto="O2",presion=640,dimPresion="mmHg",temperatura=26,dimTemperatura="C",dimVolumen="L")
    problema = problemaEsteq([dato],incognita,reaccion)
    respuestaNum, respuestaDato = problema.deCosaACosa()    
    print(respuestaDato)
    
def pruebaGas():
    gas = Gas(compuesto = "O2", magnitud = 3.15, dimVolumen="L", presion=700, dimPresion="mmHg",temperatura=30)
    print(gas)
    gas.SI()
    print(gas)
    moles = gas.aMoles()
    gas2 = Gas(compuesto = "O2", dimVolumen="L", presion=700, dimPresion="mmHg",temperatura=30)
    gas2.getIncognita(moles)
    print(gas2)
    
def pruebaReactivoLimitante():
    reaccion = "KBr + H2SO4 = Br2 + K2SO4 + SO2 + H2O"
    dato1 =  Masa(compuesto="KBr", magnitud=10, dimensional="g")
    dato2 = Solucion(compuesto="H2SO4", magnitud=5.0,dimensional="mL",molaridad=18)
    incognita = Liquido(compuesto="Br2",dimMagnitud="mL")
    problema = problemaEsteq(reaccion=reaccion,Datos=[dato1,dato2],Incognita=incognita)
    respuesta = problema.Resolver()
    print(respuesta)
    
def ejercicio3():
    reaccion = "CrI3 + Cl2 + KOH = K2CrO4 + KIO4 + KCl + H2O"
    dato1 = Masa(compuesto="CrI3", magnitud=9.45)
    dato2 = Solucion(compuesto="KOH", magnitud=789,  dimensional="mL", molaridad=0.835)
    dato3 = Gas(compuesto="Cl2",magnitud=2.6,dimVolumen="L",presion=0.784,dimPresion="atm",temperatura=306.15,dimTemperatura="K")
    Datos = [dato1,dato2,dato3]
    incisoA = problemaEsteq(Datos=Datos,Incognita=Masa(compuesto="K2CrO4"),reaccion=reaccion)
    print(incisoA.Resolver())
    incisoB = problemaEsteq(Datos=Datos,Incognita=Moles(compuesto="KIO4"),reaccion=reaccion)
    print(incisoB.Resolver())
    incisoC = problemaEsteq(Datos=Datos, Incognita=Moles(compuesto="H2O",dimensional="particulas"),reaccion=reaccion)
    print(incisoC.Resolver())

def pruebaPorcentajeRendimiento():
    reaccion = "K2Cr2O7 + SnCl2 + HCl = CrCl3 + SnCl4 + KCl + H2O"
    dato1 = Masa(compuesto="K2Cr2O7", magnitud=5.37)
    dato2 = Masa(compuesto="SnCl4", magnitud=12.5,teorico=False)
    problema = problemaEsteq(Datos=[dato1,dato2],reaccion=reaccion)
    print(problema.Resolver())
    
def ejercicio10():
    reaccion = "Zn + HNO3 = Zn(NO3)2 + H2O + NH4NO3"
    dato1 = Masa(compuesto="Zn",magnitud="3.67")
    dato2 = Solucion(compuesto="HNO3",magnitud="160",dimensional="mL",molaridad="1.3")
    dato3 = Masa(compuesto="NH4NO3",magnitud="1.05",teorico=False)
    problema = problemaEsteq(Datos=[dato1,dato2,dato3],reaccion=reaccion,tipo="Porcentaje de rendimiento")
    print(problema.Respuesta())
    
pruebaPorcentajeRendimiento()
    
def mismaCosa():
    dato1 = Gas(compuesto="H2",magnitud=200000,dimVolumen="m3",presion=0.95,dimPresion="atm",temperatura=27,dimTemperatura="C")
    incisoA = Masa(compuesto="H2",dimensional="ton")
    print(problemaEsteq([dato1],incisoA,tipo="Masa").Respuesta())
    incisoB = Moles(compuesto="H2")
    problema = problemaEsteq([dato1],incisoB,tipo="Cantidad de materia")
    print(problema.Respuesta())
    
#dato = Masa(compuesto="H2O", magnitud="30000.0",dimensional="g")
#print(dato)
#print(dato.getCifrasSig())
