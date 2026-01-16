using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.AI.TextAnalytics;

class Program
{
    static async Task Main(string[] args)
    {
        Console.Write("Who are we looking for? ");
        string? name = Console.ReadLine();
        if (name != null)
        {
            Console.WriteLine($"Right! Let's try looking for {name}");

            var client = new TextAnalyticsClient(new Uri("https://mrw-ai-dev-42.openai.azure.com/"), new AzureKeyCredential("72fe1b0d6d8e4ff5ab04619acd50a8ec"));

            var response = await client.ExtractKeyPhrasesAsync($"Tell me some top facts about {name}");

            Console.WriteLine($"Top facts about {name}:");
            foreach (var phrase in response.Value)
            {
                Console.WriteLine(phrase);
            }
        }
        else
        {
            Console.WriteLine("No name provided.");
        }
    }
}