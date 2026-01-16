using System.Threading.Tasks;
using Microsoft.Identity.Client;

namespace AzAuthenticationUsingMSAL.NET
{
    internal class Program
    {
        private const string _clientId = "clientId";
        private const string _tenantId = "tenantId";
        static async Task Main(string[] args)
        {
            try
            {
                var app = PublicClientApplicationBuilder
    .Create(_clientId)
    .WithAuthority(AzureCloudInstance.AzurePublic, _tenantId)
    .WithRedirectUri("http://localhost")
    .Build();
                string[] scopes = { "user.read" };
                AuthenticationResult result = await app.AcquireTokenInteractive(scopes).ExecuteAsync();

                Console.WriteLine($"Token:\t{result.AccessToken}");
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message); ;
            }            
        }
    }
}