using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;


namespace KeyVault_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ConfigurationController : ControllerBase
    {
        // GET: api/Configuration
        [HttpGet]
        public async Task<string> GetAsync()
        {

            try
            {
                var tokenProvider = new AzureServiceTokenProvider();
                var kvClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(tokenProvider.KeyVaultTokenCallback));
               
                var secret = await kvClient.GetSecretAsync("https://kv-testewebapp.vault.azure.net/secrets/storageAccountCnnString")
                     .ConfigureAwait(false);
                return secret.Value;
            }
            catch (KeyVaultErrorException kvException)
            {
                return kvException.Message;
            }

        }

        // GET: api/Configuration/5
        [HttpGet("{id}", Name = "Get")]
        public string Get(int id)
        {
            return "value";
        }

        // POST: api/Configuration
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT: api/Configuration/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}