using MealService.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace MealService.Infrastructure.Data
{
    public static class DatabaseInitializer
    {
        public static async Task InitializeAsync(IServiceProvider serviceProvider)
        {
            using var scope = serviceProvider.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            await context.Database.MigrateAsync();

            await SeedCategoriesAsync(context);
            await SeedCuisinesAsync(context);
            await SeedTagsAsync(context);
            await SeedMealsAsync(context);
        }

        public static async Task SeedCategoriesAsync(AppDbContext context)
        {
            if (!context.Categories.Any())
            {
                var categories = new List<Category>
                {
                    new() { Id = Guid.NewGuid(), Name = "Main dish", NormalizedName = "MAINDISH" },
                    new() { Id = Guid.NewGuid(), Name = "Healthy", NormalizedName = "HEALTHY" },
                    new() { Id = Guid.NewGuid(), Name = "Desserts", NormalizedName = "DESSERTS" }
                };

                await context.Categories.AddRangeAsync(categories);

                await context.SaveChangesAsync();
            }
        }

        public static async Task SeedCuisinesAsync(AppDbContext context)
        {
            if (!context.Cuisines.Any())
            {
                var cuisines = new List<Cuisine>
                {
                    new() { Id = Guid.NewGuid(), Name = "Italian", ImageUrl = "https://res.cloudinary.com/dlr5ta8h6/image/upload/v1742413673/Italian_zihjcm.png" },
                    new() { Id = Guid.NewGuid(), Name = "Chinese", ImageUrl = "https://res.cloudinary.com/dlr5ta8h6/image/upload/v1742413764/Chinese_mfbae9.png" },
                    new() { Id = Guid.NewGuid(), Name = "French", ImageUrl = "https://res.cloudinary.com/dlr5ta8h6/image/upload/v1742413849/French_yg3siw.pngg" }
                };

                await context.Cuisines.AddRangeAsync(cuisines);

                await context.SaveChangesAsync();
            }
        }

        public static async Task SeedTagsAsync(AppDbContext context)
        {
            if (!context.Tags.Any())
            {
                var tags = new List<Tag>
                {
                    new() { Id = Guid.NewGuid(), Name = "Spicy", Description = "Hot and spicy dishes" },
                    new() { Id = Guid.NewGuid(), Name = "Reach flavor", Description = "Exceptional taste" },
                    new() { Id = Guid.NewGuid(), Name = "Vegetarian", Description = "Meat-free options" },
                    new() { Id = Guid.NewGuid(), Name = "Gluten-Free", Description = "No gluten ingredients" }
                };

                await context.Tags.AddRangeAsync(tags);

                await context.SaveChangesAsync();
            }
        }

        public static async Task SeedMealsAsync(AppDbContext context)
        {
            if (!context.Meals.Any())
            {
                var category = context.Categories.FirstOrDefault();
                var cuisine = context.Cuisines.FirstOrDefault();
                var tags = context.Tags.ToList();

                if (category == null || cuisine == null || !tags.Any()) return;

                var meals = new List<Meal>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        Name = "Spaghetti Carbonara",
                        Price = 12.99,
                        Description = "Classic Italian pasta dish",
                        Calories = 600,
                        IsAvailable = true,
                        ImageUrl = "https://res.cloudinary.com/dlr5ta8h6/image/upload/v1742413481/Carbonara_jnr4p3.png",
                        CategoryId = category.Id,
                        CuisineId = cuisine.Id
                    }
                };

                foreach (var meal in meals)
                {
                    meal.SetTags(tags.Take(2));
                }

                await context.Meals.AddRangeAsync(meals);

                await context.SaveChangesAsync();
            }
        }
    }
}