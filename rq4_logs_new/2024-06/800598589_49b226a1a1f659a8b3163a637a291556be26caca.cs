
using System.Collections.Generic;

public class EntityManager : Singleton<EntityManager>
{
    private HashSet<Food> foodSet = new();
    private List<Food> foodList = new();

    public List<Food> Food => this.foodList;

    public void RegisterFood(Food food)
    {
        if (this.foodSet.Add(food))
        {
            this.foodList.Add(food);
        }
    }
    
    public void RemoveFood(Food food)
    {
        if (this.foodSet.Remove(food))
        {
            int index = this.foodList.IndexOf(food);
            if (index >= 0)
            {
                int lastIndex =  this.foodList.Count - 1;
                this.foodList[index] =  this.foodList[lastIndex];
                this.foodList.RemoveAt(lastIndex);
            }
        }
    }
}