using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace SoundTest
{
    static class ServerInitialiser
    {
        /// <summary>
        /// Sends a request to external server to wipe its database contents.
        /// </summary>
        /// <returns>Task handle for monitoring completion of the method.</returns>
        public static async Task ClearWavfilesDatabase()
        {
            Console.WriteLine("Attempting to erase all old files from server database.");
            using (HttpClient client = new HttpClient())
            {
                string requestUrl = "http://therentistoodamnhigh.co.uk/api.php?action=clear_all_files";
                HttpResponseMessage response = new HttpResponseMessage();
                try
                {
                    
                    response = await client.GetAsync(requestUrl);
                    string responseMessage = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Server database TRUNCATE request returned following response: "
                                      + responseMessage);
                }
                catch (HttpRequestException e)
                {
                    Console.Error.WriteLine("Exception thrown when attempting to send GET request for old files clearing: "
                                            + e.Message);
                }
            }
        }
    }
}