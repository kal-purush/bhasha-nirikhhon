using Dropbox.Api;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace CloudSearcher
{
    public class DropboxGetter
    {
        private readonly string _accessToken;
        private DropboxClient _client;

        public DropboxGetter()
        {
            _accessToken = GetAccessToken();
            InitializeClient();
        }

        private void InitializeClient()
        {
            _client = new DropboxClient(_accessToken);
        }

        private string GetAccessToken()
        {
            var parentDirectory = Directory.GetCurrentDirectory();
            var builder = new ConfigurationBuilder()
                .SetBasePath(parentDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("appsettings.Personal.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();
            return configuration["Dropbox:AccessToken"];
        }

        // Add methods to interact with Dropbox here
        // Here is a method to get all the files in a folder
        public async Task<string> GetCurrentAccountAsync()
        {
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);

                var response = await client.PostAsync("https://api.dropboxapi.com/2/users/get_current_account", null);

                response.EnsureSuccessStatusCode();

                return await response.Content.ReadAsStringAsync();
            }
        }        

        public async Task ExampleUsage()
        {
            string accountInfo = await GetCurrentAccountAsync();
            Console.WriteLine(accountInfo);
        }

    }
}