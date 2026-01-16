using artfulLifeAPI.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace artfulLifeAPI.Controllers
{
    public class RecipeController : ApiController
    {
        // GET api/recipe
        public async Task<IEnumerable<Models.Recipe>> Get()
        {
            var client = new MongoClient("mongodb://localhost/?safe=true");
            var db = client.GetDatabase("ArtfulLife");
            var recipes = db.GetCollection<Models.Recipe>("Recipes");

            var count = await recipes.CountAsync(new BsonDocument());

            //await recipes.DeleteManyAsync(new BsonDocument());

            if (count == 0)
            {
                await recipes.InsertOneAsync(new Models.Recipe()
                {
                    RecipeId = 1,
                    Ingredients = null,
                    Name = "Recipe",
                    PrepTime = 30,
                });
            }
            var filter = new BsonDocument();
            await recipes.Find(filter)
                         .ForEachAsync(doc => Console.WriteLine(doc));

            throw new NotImplementedException();
            //var context = new ArtfulLifeDataContext();

            //return from recipe in context.Recipes
            //       select new Models.Recipe()
            //       {
            //           RecipeId = recipe.Id,
            //           Name = recipe.Name,
            //           PrepTime = recipe.PrepTime,
            //       };
        }

        // GET api/recipe/5
        public Models.Recipe Get(int id)
        {
            throw new NotImplementedException();
            //var context = new ArtfulLifeDataContext();

            //return (from recipe in context.Recipes
            //        where recipe.Id == id
            //        select new Models.Recipe()
            //        {
            //            RecipeId = recipe.Id,
            //            Name = recipe.Name,
            //            PrepTime = recipe.PrepTime,
            //            Ingredients = from ingredient in context.Ingredients
            //                          where ingredient.RecipeId == recipe.Id
            //                          select new Models.Ingredient()
            //                          {
            //                              Name = ingredient.Ingredient1,
            //                          }
            //        }).FirstOrDefault();
        }

        // POST api/recipe
        public void Post([FromBody]Models.Recipe value)
        {
            throw new NotImplementedException();
            //var context = new ArtfulLifeDataContext();

            //context.Recipes.InsertOnSubmit(new Recipe()
            //    {
            //        Name = value.Name,
            //        PrepTime = value.PrepTime,
            //    });
            //context.SubmitChanges();
        }

        // PUT api/recipe/5
        public void Put(int id, [FromBody]Models.Recipe value)
        {
            throw new NotImplementedException();
            //var context = new ArtfulLifeDataContext();

            //var updatingRecipe = (from recipe in context.Recipes
            //                      where recipe.Id == id
            //                      select recipe).FirstOrDefault();
            //updatingRecipe.Name = value.Name;
            //updatingRecipe.PrepTime = value.PrepTime;
            //context.SubmitChanges();
        }

        // DELETE api/recipe/5
        public void Delete(int id)
        {
            throw new NotImplementedException();
            //var context = new ArtfulLifeDataContext();

            //var updatingRecipe = (from recipe in context.Recipes
            //                      where recipe.Id == id
            //                      select recipe).FirstOrDefault();

            //context.Recipes.DeleteOnSubmit(updatingRecipe);

            //context.SubmitChanges();
        }
    }
}