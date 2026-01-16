using DigitalFamilyCookbook.Data.Interfaces;
using DigitalFamilyCookbook.Data.Repositories;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;
using static System.Console;

namespace DataPopulator; // Note: actual namespace depends on the project name.

internal class Program
{
    static async Task Main(string[] args)
    {
        var serviceProvider = new ServiceCollection()
            .AddSingleton<IRecipeRepository, RecipeRepository>()
            .BuildServiceProvider();

        WriteLine("Please enter what you would like to create");
        WriteLine("1. Users");
        WriteLine("2. Recipes");

        int choice = int.Parse(ReadLine() ?? "0");

        switch (choice)
        {
            case 1:
                await ProcessUserCreation();
                break;
            case 2:
                ProcessRecipeCreation();
                break;
            default:
                WriteLine("Invalid Choice");
                return;
        }
    }

    private static async Task ProcessUserCreation()
    {

    }

    private static void ProcessRecipeCreation()
    {
        WriteLine("Please enter the number of recipes to create");

        string userCountInput = ReadLine() ?? "0";

        if (int.TryParse(userCountInput, out int userCount))
        {
            for (int x = 0; x < userCount; x += 1)
            {
                await
            }
        }
        else
        {
            WriteLine("Invalid Input");
            return;
        }
    }

    private static async Task AddRecipe()
    {

    }
}