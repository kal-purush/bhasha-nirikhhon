using Dapper;
using DigitalFamilyCookbook.Data.Dtos;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace DataPopulator.Data;

public static class DataService
{
    public static string ConnectionString { get; set; } = string.Empty;

    public static async Task<int> AddRecipe(RecipeDto recipe)
    {
        using var connection = new SqlConnection(ConnectionString);

        return await connection.ExecuteScalarAsync<int>(
            "[recipe].[spCreateRecipe]",
            new
            {
                recipe.Name,
                recipe.Description,
                recipe.IsPublic,
                recipe.Servings,
                recipe.Source,
                recipe.SourceUrl,
                recipe.Time,
                recipe.ActiveTime,
                recipe.ImageUrl,
                recipe.ImageUrlLarge,
                recipe.Calories,
                recipe.Carbohydrates,
                recipe.Sugar,
                recipe.Fat,
                recipe.Protein,
                recipe.Fiber,
                recipe.Cholesterol,
                recipe.UserAccountId,
                recipe.Id,
            },
            commandType: CommandType.StoredProcedure
        );
    }

    public static async Task AddStep(StepDto step)
    {
        using var connection = new SqlConnection(ConnectionString);

        await connection.ExecuteAsync(
            "[recipe].[spAddStep]",
            new
            {
                step.RecipeId,
                step.Direction,
                step.SortOrder,
                step.Id
            },
            commandType: CommandType.StoredProcedure
        );
    }

    public static async Task AddIngredient(IngredientDto ingredient)
    {
        using var connection = new SqlConnection(ConnectionString);

        await connection.ExecuteAsync(
            "[recipe].[spAddIngredient]",
            new
            {
                ingredient.RecipeId,
                ingredient.Name,
                ingredient.SortOrder,
                ingredient.Id
            },
            commandType: CommandType.StoredProcedure
        );
    }

    public static async Task<IEnumerable<CategoryDto>> GetCategories()
    {
        using var connection = new SqlConnection(ConnectionString);

        return await connection.QueryAsync<CategoryDto>(
            "[recipe].[spGetCategories]",
            commandType: CommandType.StoredProcedure
        );
    }

    public static async Task<IEnumerable<MeatDto>> GetMeats()
    {
        using var connection = new SqlConnection(ConnectionString);

        return await connection.QueryAsync<MeatDto>(
            "[recipe].[spGetMeats]",
            commandType: CommandType.StoredProcedure
        );
    }

    public static async Task AddRecipeCategory(int recipeId, int categoryId)
    {
        using var connection = new SqlConnection(ConnectionString);

        await connection.ExecuteAsync(
            "[recipe].[spAddRecipeCategory]",
            new
            {
                RecipeId = recipeId,
                CategoryId = categoryId,
            },
            commandType: CommandType.StoredProcedure
        );
    }

    public static async Task AddRecipeMeat(int recipeId, int meatId)
    {
        using var connection = new SqlConnection(ConnectionString);

        await connection.ExecuteAsync(
            "[recipe].[spAddRecipeMeat]",
            new
            {
                RecipeId = recipeId,
                MeatId = meatId,
            },
            commandType: CommandType.StoredProcedure
        );
    }
}