using Microsoft.EntityFrameworkCore;

public static class WebApplicationExtensions
{
    public static void InitializeDatabase(this WebApplication app)
    {
        using var scope = app.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TodoContext>();
        context.Database.Migrate();
        if (!context.Chores.Any())
        {
            context.Chores.AddRange(
                new Chore { Title = "Clean up the house", Person = "Liam" },
                new Chore { Title = "Take out the trash", Person = "Aria" },
                new Chore { Title = "Mow the lawn", Person = "Liam" },
                new Chore { Title = "Wash the dishes", Person = "Aria" }
            );
            context.SaveChanges();
        }
    }
}