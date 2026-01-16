# Interrogatório para a resolução de um crime


print(f'Responda a essas perguntas cuidadosamente! \nSuas respostas podem te absolver ou condenar! \n')
perguntas = [input(f'Telefonou para a vítima? S N: '),
             input(f'Esteve no local do crime? S N: '),
             input(f'Devia a vítima? S N: '),
             input(f'Trabalhava com a vítima? S N: '),
             input(f'Mora perto da vítima? S N: ')]

contador = perguntas.count('s')

print('\nPelas suas respoastas você tem grandes chances de ser: \n')

if contador == 2:
    print(f'Suspeito!')
elif 3 <= contador <= 4:
    print(f'Cumplice!')
elif contador == 5:
    print(f'Culpado!')
else:
    print(f'Inocente!')