using System;
using Microsoft.Extensions.Configuration;

namespace TS.StandaloneKestrelServer
{
    public class StandaloneKestrelServerConfigurationLoader
    {
        public IConfiguration Configuration { get; }
            
        public bool ReloadOnChange { get; }
            
        public StandaloneKestrelServerConfigurationLoader(IConfiguration configuration, bool reloadOnChange)
        {
            Configuration = configuration;
            ReloadOnChange = reloadOnChange;
        }

        public string GetServerType()
        {
            return Configuration.GetValue<string>("ServerType");
        }
    }
}