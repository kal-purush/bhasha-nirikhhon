

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AutomativeTrippinUserCreator
{
    class Program
    {
        private static readonly HttpClient client = new HttpClient() { BaseAddress = new Uri("https://services.odata.org/TripPinRESTierService/(S(iealvvsxhq1332zfrt0i4u2r))/") };
        static async Task Main(string[] args)
        {
            try
            {
                var text = System.IO.File.ReadAllText(@"users.json");
                var results = JsonSerializer.Deserialize<IEnumerable<User>>(text);
                foreach (var result in results)
                {
                    HttpResponseMessage response = await client.GetAsync($"People('{result.UserName}')");
                    if (!response.IsSuccessStatusCode)
                    {
                        HttpResponseMessage postResponse = await client.PostAsync("People",
                            new StringContent(JsonSerializer.Serialize(new
                            {
                                result.UserName,
                                result.FirstName,
                                result.LastName,
                                Emails = new List<string> { result.Email },
                                AddressInfo = new List<object>{
                                    new
                                    {
                                        result.Address,
                                        City = new
                                        {
                                            Name = result.CityName,
                                            CountryRegion = result.Country,
                                            Region = result.Country
                                        }
                                    }
                                }
                            }), Encoding.UTF8, "application/json"));
                        Console.WriteLine($"Add User: {result.UserName}");
                    }
                    else
                    {
                        Console.WriteLine($"Already added User: {result.UserName}");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }


        }
    }
}