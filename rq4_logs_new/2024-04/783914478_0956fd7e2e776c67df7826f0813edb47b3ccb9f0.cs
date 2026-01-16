using MySql.Data.MySqlClient;
using Microsoft.Extensions.Configuration;

namespace DataLayer;

public class DatabaseConnection : IAsyncDisposable
{
    public readonly MySqlConnection Connection;

    public DatabaseConnection()
    {
        
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
        
        
        Connection = new MySqlConnection(configuration.GetConnectionString("DefaultConnection"));

        try
        {
            Connection.Open();
            
            Console.WriteLine("Connected to database.");
        }
        catch (MySqlException exception)
        {
            Console.WriteLine("Error: " + exception.Message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Connection.CloseAsync();
        
        Console.WriteLine("Disconnected from database.");
    }
}