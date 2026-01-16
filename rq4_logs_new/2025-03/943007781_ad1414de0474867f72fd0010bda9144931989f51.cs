using System;
using MySql.Data.MySqlClient;

public class DatabaseHelper
{
    private string connectionString = "server=localhost;database=world;user=root;password=;";

    public void GetCountriesByPopulation()
    {
        using (MySqlConnection conn = new MySqlConnection(connectionString))
        {
            try
            {
                conn.Open();
                string query = "SELECT Code, Name, Continent, Region, Population, Capital FROM country ORDER BY Population DESC;";

                using (MySqlCommand cmd = new MySqlCommand(query, conn))
                {
                    using (MySqlDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("\nCountries Ordered by Population:");
                        Console.WriteLine("------------------------------------------------------");

                        while (reader.Read())
                        {
                            Console.WriteLine($"Code: {reader["Code"]}, Name: {reader["Name"]}, Continent: {reader["Continent"]}, " +
                                              $"Region: {reader["Region"]}, Population: {reader["Population"]}, Capital: {reader["Capital"]}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
    }

    public void GetTopCitiesInContinent(string continent, int topN)
    {
        using (MySqlConnection conn = new MySqlConnection(connectionString))
        {
            try
            {
                conn.Open();
                string query = $"SELECT city.Name, city.CountryCode, city.Population FROM city " +
                               $"JOIN country ON city.CountryCode = country.Code " +
                               $"WHERE country.Continent = @Continent ORDER BY city.Population DESC LIMIT @TopN;";

                using (MySqlCommand cmd = new MySqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@Continent", continent);
                    cmd.Parameters.AddWithValue("@TopN", topN);

                    using (MySqlDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine($"\nTop {topN} Cities in {continent}:");
                        Console.WriteLine("------------------------------------------------------");

                        while (reader.Read())
                        {
                            Console.WriteLine($"Name: {reader["Name"]}, Country: {reader["CountryCode"]}, Population: {reader["Population"]}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
    }

    public void GetLanguageSpeakers()
    {
        using (MySqlConnection conn = new MySqlConnection(connectionString))
        {
            try
            {
                conn.Open();
                string query = "SELECT Language, SUM((Population * Percentage) / 100) AS TotalSpeakers FROM countrylanguage " +
                               "JOIN country ON countrylanguage.CountryCode = country.Code " +
                               "WHERE Language IN ('Chinese', 'English', 'Hindi', 'Spanish', 'Arabic') " +
                               "GROUP BY Language ORDER BY TotalSpeakers DESC;";

                using (MySqlCommand cmd = new MySqlCommand(query, conn))
                {
                    using (MySqlDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("\nLanguage Speaker Statistics:");
                        Console.WriteLine("---------------------------------");

                        while (reader.Read())
                        {
                            Console.WriteLine($"Language: {reader["Language"]}, Total Speakers: {reader["TotalSpeakers"]}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("" +
                    "Error: " + ex.Message);
            }
        }
    }
}