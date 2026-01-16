import os
import game_functions as func

screen = {
    "linha_1": "     |     |     ",
    "linha_2": "  1  |  2  |  3  ",
    "linha_3": "_____|_____|_____",
    "linha_4": "     |     |     ",
    "linha_5": "  4  |  5  |  6  ",
    "linha_6": "_____|_____|_____",
    "linha_7": "     |     |     ",
    "linha_8": "  7  |  8  |  9  ",
    "linha_9": "     |     |     ",
}


####################################################
os.system('clear')
while True:
    func.update_screen()
    option = input('Selecione um número de 1 a 9 para jogar. ')

    if option == '1' or option == '2' or option == '3':
        option_in_line = 2
        linha = func.line_transformer(option_in_line, option)
        modified_line = func.mark_one(option_in_line, linha, option)
        func.update_linha(modified_line, option_in_line)
        
        os.system('clear')
        
    elif option == '4' or option == '5' or option == '6':
        option_in_line = 5
        linha = func.line_transformer(option_in_line, option)
        modified_line = func.mark_one(option_in_line, linha, option)
        func.update_linha(modified_line, option_in_line)
        
        #linha = line_transformer(option_in_line, option)
        #modified_line = mark_one(option_in_line, linha, option)
        #update_linha(modified_line, option_in_line)

        os.system('clear')

    elif option == '7' or option == '8' or option == '9':
        option_in_line = 8
        linha = func.line_transformer(option_in_line, option)
        modified_line = func.mark_one(option_in_line, linha, option)
        func.update_linha(modified_line, option_in_line)
        
        #linha = line_transformer(option_in_line, option)
        #modified_line = mark_one(option_in_line, linha, option)
        #update_linha(modified_line, option_in_line)
        
        os.system('clear')
        
    else:
        os.system('clear')        
        print('Insira um valor válido!')
    
