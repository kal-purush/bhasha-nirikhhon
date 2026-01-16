using BudgetTourist.Contracts.FoodContracts;
using System.Collections.Generic;


namespace BudgetTourist.Models.FoodModels
{
    public class FoodOrder
    {
        private double totalPrice = 0;
        

        IList<IConsumable> orderList { get; set; }
        public double TotalPrice
        {
            get
            {
                return this.totalPrice;
            }
            private set
            {
                foreach (var dish in orderList)
                {
                    totalPrice += dish.Price;
                }
            }
        }
        
    }
}