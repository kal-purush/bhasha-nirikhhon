class terminal:
    def __init__(self,id,demanda,posx,posy):
        self.id = id
        self.demanda=demanda
        self.posx=posx
        self.posy=posy
    
    def __str__(self):
        s = str(self.id)+" "+str(self.demanda)
        return s
class concentrator:
    def __init__(self,id,capacidad,posx,posy):
        self.id = id
        self.capacidad=capacidad
        self.posx=posx
        self.posy=posy
    
    def __str__(self):
        s = str(self.id)+" "+str(self.capacidad)
        return s



terminales=[]
concentradores=[]
terminales.append(terminal(1,5,54,28))
terminales.append(terminal(2,4,28,75))
terminales.append(terminal(3,4,84,44))
terminales.append(terminal(4,2,67,17))
terminales.append(terminal(5,3,90,41))
terminales.append(terminal(6,1,68,67))
terminales.append(terminal(7,3,24,79))
terminales.append(terminal(8,4,38,59))
terminales.append(terminal(9,5,27,86))
terminales.append(terminal(10,4,7,76))

concentradores.append(concentrator(1,12,19,76))
concentradores.append(concentrator(2,14,50,30))
concentradores.append(concentrator(3,13,23,79))

print(concentradores[1].posx)