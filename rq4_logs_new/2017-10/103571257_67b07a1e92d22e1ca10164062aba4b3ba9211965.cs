using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Entity;
using FoodProject.Models;
using FoodProject.Concrete;

namespace FoodProject
{
    public class FoodInitializer : System.Data.Entity.DropCreateDatabaseIfModelChanges<FoodContext>
    {
        protected override void Seed(FoodContext context)
        {
            var users = new List<User>
            {
                new User{Name="Ryan",Password="Ryan"},
                new User{Name="user",Password="password"}
            };

            users.ForEach(u => context.Users.Add(u));
            context.SaveChanges();


            var foods = new List<Food>
            {
                new Food{Name="Milk", Units=1},
                new Food{Name="Bread", Units=1},
                new Food{Name="Cheese", Units=1},
                new Food{Name="Ham", Units=1},
                new Food{Name="Lettuce", Units=1}
            };
            foods.ForEach(f => context.Foods.Add(f));
            context.SaveChanges();


            var pantry = new List<Pantry>
            {
                new Pantry{UserID=1, FoodID=1},
                new Pantry{UserID=1, FoodID=2},
                new Pantry{UserID=1, FoodID=3},
                new Pantry{UserID=2, FoodID=4},
                new Pantry{UserID=2, FoodID=5},
            };
            pantry.ForEach(p => context.Pantrys.Add(p));
            context.SaveChanges();

            var recipe = new List<Recipe>
            {
                new Recipe{Name="Ham Sandwich"},
                new Recipe{Name= "Milky Ham"}
            };
            recipe.ForEach(r => context.Recipes.Add(r));
            context.SaveChanges();

            var ingredient = new List<Ingredient>
            {
                new Ingredient{FoodID=2, RecipeID=1},
                new Ingredient{FoodID=4, RecipeID=1},
                new Ingredient{FoodID=1, RecipeID=2},
                new Ingredient{FoodID=4, RecipeID=2}
            };
            ingredient.ForEach(i => context.Ingredients.Add(i));
            context.SaveChanges();
        }
    }
}