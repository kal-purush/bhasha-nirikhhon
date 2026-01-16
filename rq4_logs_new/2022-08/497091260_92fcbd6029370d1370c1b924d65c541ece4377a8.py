Historial3 = (9530, 4120, 4580, 1500, 890,7516,426)
cant_serv = len(Historial3)
monto_total= 0
for monto in Historial3:
    monto_total += monto
    
gasto_prom = monto_total / cant_serv
print(gasto_prom)
if gasto_prom > 4500:
    print("Se ha excedido del gasto promedio para su mascota")