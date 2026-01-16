using System.Text;
using Newtonsoft.Json;

namespace OuterTest;

class Program
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string apiUrl = "https://localhost:7106/api/Book";

    static async Task Main(string[] args)
    {
        while (true)
        {
            Console.WriteLine("Введите действие:");
            Console.WriteLine("1) Добавить 100 элементов");
            Console.WriteLine("2) Добавить 100000 элементов");
            Console.WriteLine("3) Удалить все элементы (Id от 0 до 100000)");
            Console.WriteLine("4) Выйти");
            string choice = Console.ReadLine();

            switch (choice)
            {
                case "1":
                    await AddElements(100);
                    break;
                case "2":
                    await AddElements(100000);
                    break;
                case "3":
                    await DeleteElements();
                    break;
                case "4":
                    return; // Выход из программы
                default:
                    Console.WriteLine("Неправильный ввод, попробуйте снова.");
                    break;
            }
        }
    }

    static async Task AddElements(int count)
    {
        for (int i = 0; i < 100_000; i++)
        {
            var book = new
            {
                Id = i,
                Name = $"Book {i}",
                Description = $"Description for book {i}",
                Author = $"Author {i}"
            };

            var json = JsonConvert.SerializeObject(book);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var response = await client.PostAsync(apiUrl, content);

            if (response.IsSuccessStatusCode)
            {
                //Console.WriteLine($"Successfully added book {i}");
            }
            else
            {
                Console.WriteLine($"Failed to add book {i}");
            }
        }
        Console.WriteLine($"Ended");
    }
    
    static async Task DeleteElements()
    {
        for (int i = 0; i < 1000; i++)
        {
            var response = await client.DeleteAsync($"{apiUrl}/{i}");

            /*if (response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Successfully deleted book {i}");
            }
            else
            {
                Console.WriteLine($"Failed to delete book {i}");
            }*/
        }
        Console.WriteLine($"Ended");
    }
}