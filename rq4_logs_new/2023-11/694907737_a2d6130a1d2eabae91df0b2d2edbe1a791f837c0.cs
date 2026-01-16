

using Microsoft.Identity.Client;
/** 
* AUTHOR: Halmar Arteaga
* DATE: 11/05/2023
* LAST UPDATED: 11/02/2023
*
* NOTES: This file contains methods needed to get JSON data from the DND5E API... will eventually type cast them into C# objects
* 
* API: https://www.dnd5eapi.co/
*
* USAGE: //make new instance of DNDAPI
*     var api = new DNDAPI();
*     
*     //Call GetSpell method
*     await api.GetSpell("Acid Arrow");
*     
*     //Get JSON response in JSONResponse
*     api.JSONResponse; //<-- response is here
*/
namespace DnD_NPC_Generator.WebAPI
{
    public class DNDAPI
    {
        public string JSONResponse { get; set; }

        private static HttpClient client = new()
            {
                BaseAddress = new Uri("https://www.dnd5eapi.co/api/")
            };

        //Get Spell JSON string from the API
        public async Task GetSpell(string spell)
        {
            string search = "spells/";
            string term = spell.ToLower().Replace(" ", "-");
            HttpResponseMessage response = await client.GetAsync(search + term);

            if(response.IsSuccessStatusCode)
            {
                JSONResponse = await response.Content.ReadAsStringAsync();
            } else
            {
                Console.WriteLine($"Error: {spell} was not found in API");
                JSONResponse = null;
            }
        }

        //Get Feature JSON string from the API
        public async Task GetFeature(string feature)
        {
            string search = "features/";
            string term = feature.ToLower().Replace(" ", "-");
            HttpResponseMessage response = await client.GetAsync(search + term);

            if (response.IsSuccessStatusCode)
            {
                JSONResponse = await response.Content.ReadAsStringAsync();
            } else
            {
                Console.WriteLine($"Error: {feature} was not found in API");
                JSONResponse = null;
            }
        }
    }
}