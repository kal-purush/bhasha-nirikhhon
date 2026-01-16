using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebWasted.Timers
{
    public class FoodExpiryTimer
    {
        public FoodExpiryTimer()
        {

        }
        public void RemoveExpiredFood(Object stateInfo)
        {
            lock (DatabaseHandler.Instance.dc)
            {
                var foods = DatabaseHandler.Instance.dc.Foods.ToList();
                var expiredFoods = new List<Food>();

                foreach(Food food in foods)
                {
                    if ((food.ExpDate-DateTime.Now).TotalDays <= 0.0)
                    {
                        expiredFoods.Add(food);
                    }
                }
                if (expiredFoods != null)
                {
                    DatabaseHandler.Instance.dc.Foods.RemoveRange(expiredFoods);
                    DatabaseHandler.Instance.dc.SaveChanges();
                }
            }
            
            
        }
           
    }
}