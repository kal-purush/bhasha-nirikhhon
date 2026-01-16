using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Nexus.Link.Libraries.Core.Platform.Configurations;

namespace Nexus.Link.Libraries.Web.Clients
{
    public static class ClientConfigurationHelper
    {
        public static ClientConfiguration GetConfigurationForClient(ILeverConfiguration configuration, string client)
        {
            var clientConfigurations = GetClientsConfigurations(configuration);
            if (clientConfigurations == null) return null;

            return clientConfigurations.TryGetValue(client, out var clientConfiguration) ? clientConfiguration : null;
        }

        public static Dictionary<string, ClientConfiguration> GetClientsConfigurations(ILeverConfiguration configuration)
        {
            var tenantClientSettingJToken = configuration?.Value<JToken>("Clients");
            if (tenantClientSettingJToken == null) return null;
            var clientConfigurations = JsonConvert.DeserializeObject<List<ClientConfiguration>>(tenantClientSettingJToken.ToString());
            return clientConfigurations.ToDictionary(x => x.Name, x => x);
        }

    }
}