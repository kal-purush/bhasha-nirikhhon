class Food:
    def __init__(self, name:str, price:float, description:str):
        self.name = name
        self.price = price 
        self.description = description


pizza = Food("Margherita", 8.00, "mozzarella, pomodoro, basilico")
cheesecake = Food("Cheesecake", 6.00, "philadelphia, burro, marmellata")
pasta = Food("Carbonara", 10.00, "uova, guanciale, pepe, pecorino")


class Menu:
    def __init__(self) -> list:
        self.foods = []
    
    def addFood(self, food:str):
        self.foods.append(food)

    def removeFood(self, food:str):
        if self.food in self.foods:
            self.foods.remove(food)
        
    def printPrices(self):
        for food in self.foods:
            print(f"Descrizione:\nPiatto: {food.name}\nPrezzo: {food.price}.\n")
    
    def getAveragePrice(self):
        if len(self.foods) == 0:
            return 0
        else: 
            prezzo_totale = sum(food.price for food in self.foods)
            return prezzo_totale / len(self.foods)

menu = Menu()

menu.addFood(pizza)
menu.addFood(cheesecake)
menu.addFood(pasta)

insalata = Food("insalata", 6.50, "riso, pomodoro, olive nere")
menu.addFood(insalata)
bistecca = Food("bistecca", 18.00, "Carne di manzo, sale, pepe")
menu.addFood(bistecca)

print("Prezzi Menu: \n")
menu.printPrices()

prezzo_medio = menu.getAveragePrice()
print(f"\nIl prezzo medio del Menu è: {prezzo_medio:.2f}.")