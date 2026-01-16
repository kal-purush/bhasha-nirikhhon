using FridgeWPF.ConcreteIngredients;
using System;

namespace FridgeWPF
{
    public class CabbageFactory : AbstractIngredientFactory
    {
        public override string Name { get; protected set; } = "Cabbage";

        public override AbstractIngredient Create(double amount, DateTime expiryDate)
        {
            return new Cabbage(amount, expiryDate);
        }

        public override AbstractIngredient Create(double amount)
        {
            return new Cabbage(amount);
        }
    }
}
