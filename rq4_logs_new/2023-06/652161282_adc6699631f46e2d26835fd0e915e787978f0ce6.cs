using Microsoft.EntityFrameworkCore;
using Services.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Services.Service
{
    public class MealPlanningRepository : RepositoryBase<MealPlanning>
    {
        Recipe_Organizer_PRN211Context _context;
        DbSet<MealPlanning> _dbSet;

        public MealPlanningRepository()
        {
            _context = new Recipe_Organizer_PRN211Context();
            _dbSet = _context.Set<MealPlanning>();
        }
        public List<MealPlanning> getListRecipesByUserId (int userId) =>
            _dbSet.Where(l => l.UserId == userId).ToList();
        public void SavePlan (List<MealPlanning> listRecipes, int userId)
        {
            List<MealPlanning> mealPlannings = getListRecipesByUserId(userId);
            if (listRecipes.Count == 0) {
                foreach (var plan in mealPlannings)
                {
                    Delete(plan);
                }
            } else
            {
                if (mealPlannings.Count == 0)
                {
                    foreach (var plan in listRecipes)
                    {
                        Add(plan);
                    }
                } else
                {
                    List<MealPlanning> exist = new List<MealPlanning>();
                    foreach(var plan in listRecipes)
                    {
                        List<MealPlanning> exist2 = new List<MealPlanning>();
                        foreach( var plan2 in mealPlannings)
                        {

                            if (plan.Session.Equals(plan2.Session) 
                                && plan.WeekStartDate.Equals(plan2.WeekStartDate)
                                && plan.RecipeId.Equals(plan2.RecipeId))
                            {
                                if (exist.Count == 0)
                                {
                                    exist.Add(plan);
                                    exist2.Add(plan2);
                                }
                                if (exist.Count > 1)
                                {
                                    exist2.Add(plan2);
                                }
                            }
                        }
                        if (exist2.Count > 0)
                        {
                            foreach( var plan2 in exist2)
                            {
                               
                                mealPlannings.Remove(plan2);
                            }
                        }
                    }
                    if (mealPlannings.Count > 0)
                    {
                        foreach ( var plan in mealPlannings)
                        {
                            Delete(plan);
                        }
                    }
                    if (exist.Count > 0)
                    {
                        foreach (var plan in exist)
                        {
                            listRecipes.Remove(plan);
                        }
                    }
                    if (listRecipes.Count > 0)
                    {
                        foreach(var plan in listRecipes)
                        {
                            Add(new MealPlanning()
                            {
                                RecipeId = plan.RecipeId,
                                UserId = plan.UserId,
                                WeekStartDate = plan.WeekStartDate,
                                Session = plan.Session
                            });
                        }
                    }



                }
            }
        }
    }
}