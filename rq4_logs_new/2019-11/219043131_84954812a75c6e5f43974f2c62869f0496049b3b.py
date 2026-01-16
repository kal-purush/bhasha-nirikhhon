a = "I am global variable!"


def enclosing_funcion():
    
    #a = "I am variable from enclosed function!"    #task4.4.2.1
    
    def inner_function():
        
        #a = "I am local variable!"     #task4.4.2.2
        print(a)
        
    return inner_function               #task 4.4.1

if __name__ == "__main__":
    fun = enclosing_funcion()
    fun()