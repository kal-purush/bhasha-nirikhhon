import random as rnd    
def win(p1,pc):
    if (p1 == 'rock' and pc == 'scissors') or (p1 == 'scissors' and pc == 'paper') or (p1 == 'paper' and pc == 'rock'): 
        return True
while True:
    try:
        print("-"*50)
        print("Quit: 'q' or 'quit'")
        select = input("Enter 'rock','paper' or 'scissors': ").lower()
        computer = rnd.choice(['rock','paper','scissors'])
        if (select == 'rock') or select == 'paper' or select == 'scissors' or select == 'q' or select == 'quit':
            print(f"Computer's choice: {computer}")
            if select == 'q' or select == 'quit':
                break
            else:
                if win(select,computer):
                    print('YOU WON')
                elif select == computer:
                    print('TIE')
                else:
                    print('YOU LOSE')
        else:
            raise Exception("Please enter a valid word(ex:rock/paper/scissors/q-quit)")
    except Exception as error:
        print(f"{error}")
    