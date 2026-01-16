import os

def gravar():
    path = os.listdir('.')

    for indice, file in enumerate(path):
        if file.startswith('handler.py'):
            continue

        else:
            os.rename(file, f"Faixa {indice}.mp3")
    
    print('FEITO!')
    print(os.listdir('.'))


def main():
    gravar()
    input('pressione ENTER para sair...')


main()