# implementacion de un lenguaje basico (gdos), basado en BASIC, con la finalidad de comprobar el interprete

import sys
sys.path.insert(0,"../..")

if sys.version_info[0] >= 3:
    raw_input = input

import gdoslex
import gdosparse
import gdosinterp

#Si se especifica el nombre de un programa se intentara ejecutar
#en caso de ocurrir un error se captura y se entra en el modo interactivo.

if len(sys.argv) == 2:
    data = open(sys.argv[1]).read()
    prog = gdosparse.parse(data)
    if not prog: raise SystemExit
    p = gdosinterp.GdosInterpreter(prog)
    try:
        p.run()
        raise SystemExit
    except RuntimeError:
        pass

else:
    p = gdosinterp.GdosInterpreter({})

#El modo interactivo funciona de una manera similar al interprete de python,
#te permite modificar el programa por defecto en BasicInterpreter(),
#se debe especificar el numero de linea de cada comando, para que el interprete no borre la linea anterior.
#En este modo se agregan los comandos 'nuevo', 'lista' y 'correr'.

print 'Bienvenido al modo interactivo de gdos; Desarrollado por Grupo Dos de Traductores e Interpretes\n 6/07/2015 Ver. 0.0.1'

while 1:
    try:
        line = raw_input(">> ")
    except EOFError:
        raise SystemExit
    if not line: continue
    elif line == "salir":
        sys.exit(1)
    line += "\n"
    prog = gdosparse.parse(line)
    if not prog: continue

    keys = list(prog)
    if keys[0] > 0:
         p.add_statements(prog)
    else:
        stat = prog[keys[0]]
        if stat[0] == 'CORRER':
            try:
                p.run()
            except RuntimeError:
                pass
        elif stat[0] == 'LISTA':
            p.list()
        elif stat[0] == 'BORRAR':
            p.del_line(stat[1])
        elif stat[0] == 'NUEVO':
            p.new()