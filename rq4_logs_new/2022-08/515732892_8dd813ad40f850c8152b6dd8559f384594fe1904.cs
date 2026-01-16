using System;
using System.Linq;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace jokesHTTPClient
{
    class Program
    {
        //Create Http client object
        HttpClient client = new HttpClient();
        
        // Method must be async if we want to wait for it to finish
        //In order to await it the method must be async
        //Task because it can not return void, for some reason
        static async Task Main(string[] args)
        {
            //Access 
            Program program = new Program();
            await program.GetJokes();

        }         
        //In order to await it the method must be async
        //await is used to wait for it finish
        private async Task GetJokes()
        {
            //Our response will be parsed into a string
            string response = await client.GetStringAsync(
                "http://api.icndb.com/jokes/15?firstName=Tony&lastName=Stark");

            string response2 = await client.GetStringAsync(
                "http://api.icndb.com/jokes/random/3?firstName=Mark&lastName=Moore");

            
            //output both links
            Console.WriteLine(response);
            Console.WriteLine("");
            Console.WriteLine(response2);
        }



    }                       
}