# Autor: Ronaldo Estefano Lira Buendia
# Calcular el rendimiento en Km/lit y mill/gal de un carro

def rendimientoKmLt(km, lit):
    kmLit = km / lit
    return kmLit

def rendimientoMillGal(km, lit):
    milGal = (km/1.6093)/(lit*.264)
    return milGal

def recorrerKm(rekm, renKm):
    kmAlit = rekm / renKm
    return kmAlit


def main():
    km = int(input("Teclea el numero de km recorridos: "))
    lit = int(input("Teclea el numero de litros de gasolina usados: "))
    renKm = rendimientoKmLt(km, lit)
    renMill = rendimientoMillGal(km, lit)
    print("Si recorres ",km ,"kms con ",lit ,"litros de gasolina, el rendimiento es: ");print("{0:.2f}".format(renKm),"km/l");print("{0:.2f}".format(renMill),"mi/gal")
    rekm = int(input("Cuantos kilometros vas a recorrer? "))
    recoKm = recorrerKm(rekm, renKm)
    print("Para recorrer ",rekm ,"km. necesitas {0:.2f}".format(recoKm) ,"litros de gasolina")



main ()