import os
import sys
class Node(): 
    def __init__(self, data):
        self.data = data 
        self.next = None 
    
        
class Rorschach(): 
    
    def __init__(self): 
        self.root = None 
        self.end = None 
        self.ended = None 
    
    def Null(self): 
        if self.root == None: 
            return True 
        else:
            return False 
        
    def push(self,data): 
        
        if self.Null() == True: 
            self.root = self.end = self.ended = Node(data)
            self.end.next = self.ended.next = self.root
        else:
            temporal = self.ended
            self.end = self.ended = temporal.next = Node(data)
            self.end.next = self.ended.next = self.root
            
    def peek(self): 
        temp = self.root
        i = True
        if self.Null() == True: 
            print("\nNo existen datos. ") 
        else:
            while i: 
                print("[" + str(temp.data)+"] ")
                if temp == self.ended:
                    i = False
                else:    
                    temp = temp.next
    
    def Search(self,item): 
        self.end = self.root
        temp = 1
        if self.Null() == True:
                    print("\nLa lista esta vacia.")
        else:
            while self.end.data != item and temp != 1:  
                if self.end.next != self.root:  
                    self.end = self.end.next
                else:
                    temp = 0
            if temp==0:
                print("\nNo existe el dato en la lista.")
            else:
                print("\nEl dato existe en la lista.")
    
class  Menu(): 
    
    def __init__(self): 
        self.op = None
        
    def fill(self): 
        filled = Rorschach() 
        while True: 
            print("\n1.-Guardar datos\n2.-Mostrar datos\n3.-Buscar dato\n4.-Salir")
            self.op = int(input("\nIngrese opcion: ")) 
            os.system("clear") 
            
            if self.op == 1:
                x = input("\nIngresa dato: ")
                filled.push(x)
            
            elif self.op == 2:
                filled.peek() 
            
            elif self.op == 3:
                date = input("\nIngresa dato a buscar: ")
                check = filled.Search(date) 
                
                    
            elif self.op == 4:
                sys.exit() 
                
            else: 
                print("\nOpcion invalida")


Imprimir = Menu() 
Imprimir.fill()