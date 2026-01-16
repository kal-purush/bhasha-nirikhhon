fileName = 'data'

class BoxGenerator(object):
    """docstring for BoxGenerator."""
    def __init__(self, nameAlgo, population, data):
        if nameAlgo != "Calculate":
            if nameAlgo == "Existence"
                print ("Population generator for genetic search with existence verification.");

                self.generate = function randomPopulation(population, data['numIndi'], chromSize, info) {
                picked = None
                chromosome = None
                count = None
                index = None
                selecting = None

                items = None
                for (var i = 0; i < popSize; ++i) {
                    items = chance.integer({ min: 1, max: 3 });
                    picked = new Set();
                    chromosome = new Array(chromSize).fill(0);
                    count = 0;

                    // Iterate a from 1 to 3 times max
                    for (var j = 0; j < items; ++j) {
                        selecting = true;
                        while (selecting) {

                            index = chance.integer({ min: 0, max: (chromSize - 1) });

                            // Won't modify the index if it has already been chosen
                            if (!picked.has(index) && chromosome[index] <= info) {
                                selecting = false;



                                picked.add(index);
                            }
                        }

                        chromosome[index] = 1;
                    }

                    var box = new Box(chromosome);
                    population.push(box);

        self.nameAlgo = nameAlgo

def randomPopulation(population, popSize, chromSize, searchType):
    pass

def matrixLoad():
    matrix = []
    with open(fileName) as text:
        lines = text.readlines()

        for line in lines:
            l = line.split()
            cols = [int(i) for i in l[:-1]]
            cols.append(l[-1])

            matrix.append(cols)

    return(matrix)

def showData(data):
    for key in data:
        print (key,':',data[key])

def showTable():
    a = matrixLoad()
    print ()
    for i in a:
        print(i)
    print ()

def modifyWeight():
    return int(input('Introduce nuevo peso maximo: \n\t =>'))

def modifyIndividuals():
    return int(input('Introduce nuevo numero de individuos: \n\t =>'))

def modifyGenerations():
    return int(input('Introduce nuevo numero de generaciones: \n\t =>'))

def modifyReproduce():
    return int(input('Introduce nuevo numero de reproducciones: \n\t =>'))

def printAlgoInfo(nameAlgo,data):
    print ("Calcular por la opcion",nameAlgo+'.')
    population= []
    showData(data)

def main():
    table = matrixLoad()
    data= {'weight' : 17,
    'numIndi' : 8,
    'numGene' : 10,
    'numRepro' : 3}

    while True:
        print ("Menu")

        print ("1.- Ver datos")
        print ("2.- Ver tabla de datos")
        print ("3.- Modificar peso maximo")
        print ("4.- Numero de individuos por generacion")
        print ("5.- Numero de generaciones")
        print ("6.- Numero de individuos que reproducira")
        print ("7.- CALCULAR")
        print ("8.- EXISTENCIA")
        print ("9.- TIPO")
        print ("10.- Salir")

        opc = int(input('Introduce numero: \n\t =>'))

        if opc == 1:
            showData(data)
        elif opc == 2:
            showTable()
        elif opc == 3:
            data['weight'] = modifyWeight()
        elif opc == 4:
            data['numIndi'] = modifyIndividuals()
        elif opc == 5:
            data['numGene'] = modifyGenerations()
        elif opc == 6:
            data['numRepro'] = modifyReproduce()
        elif opc == 7:
            printAlgoInfo("Calculate",data)
        elif opc == 8:
            printAlgoInfo("Existence",data)
        elif opc == 9:
            printAlgoInfo("Type",data)
        elif opc == 10:
            break
        else:
            print ("Opcion incorrecta")

if __name__ == '__main__': main()