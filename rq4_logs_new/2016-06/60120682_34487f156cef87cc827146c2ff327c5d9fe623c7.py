def leEntrada(nome):
    f = open(nome, "r")
    tipo_grade = 0;
    cont = 0;
    Posicoes[]

    for linha in f:
        if cont == 0:
            if linha == "H":
                tipo_grade = 1
            cont = cont + 1
        else:
            
        print(linha)
    f.close()

def main():
    leEntrada("arquivo.txt");

main()