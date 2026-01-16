using Newtonsoft.Json;

namespace TCObjectStorageClient.Models
{
    public class CreateTokenRequest
    {
        public class Authentication
        {
            [JsonProperty("tenantName")]
            public string TenantName { get; set; }
            [JsonProperty("passwordCredentials")]
            public PasswordCredentials PasswordCredentials { get; set; } = new PasswordCredentials();
        }

        public class PasswordCredentials
        {
            [JsonProperty("username")]
            public string UserName { get; set; }
            [JsonProperty("password")]
            public string Password { get; set; }
        }

        [JsonProperty("auth")]
        public Authentication Auth { get; set; } = new Authentication();
    }
}