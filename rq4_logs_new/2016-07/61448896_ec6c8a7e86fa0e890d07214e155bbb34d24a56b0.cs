namespace ProjectFood
{
    using Contracts;
    using Enums;
    using Models;
    
    public class MenuMaker
    {
        public IFood CreateApetizer(string dishName, DishType dishType,double portionSize,double price)
        {
            return new Appetizer(dishName,dishType,portionSize,price);
        }
        public IFood CreateMainDish(string dishName, DishType dishType, double portionSize, double price)
        {
            return new MainDish(dishName, dishType, portionSize, price);
        }
        public IFood CreateDessert(string dishName, DishType dishType, double portionSize, double price)
        {
            return new Dessert(dishName, dishType, portionSize, price);
        }
        public IDrink CreateDrink(string dishName, DrinkType drinkType, double portionSize, double price)
        {
            return new Drink(dishName, drinkType, portionSize, price);
        }
    }
}