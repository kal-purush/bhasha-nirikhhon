from funcionarios import reajusta_dez_porcento

def main():

    lista_empregados = [
        {"nome": "Nome1", "sobrenome": "Sobrenome1", "ano_nascimento": 1990, "RG": "123456", "ano_admissao": 2010, "salario": 5000.0},
        {"nome": "Nome2", "sobrenome": "Sobrenome2", "ano_nascimento": 1985, "RG": "654321", "ano_admissao": 2015, "salario": 6000.0}
    ]

    print("Lista de Empregados antes do reajuste:")
    for empregado in lista_empregados:
        print(empregado)

    reajusta_dez_porcento(lista_empregados)

    print("\nLista de Empregados após o reajuste de 10%:")
    for empregado in lista_empregados:
        print(empregado)

if __name__ == "__main__":
    main()