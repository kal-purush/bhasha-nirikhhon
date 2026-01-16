using XModPackager.Config.Models;
using XModPackager.Options;

namespace XModPackager.Build
{
    public class BuildContext
    {
        public ConfigModel Config {get; private set;}
        public BuildOptions Options {get; private set;}

        public BuildContext(ConfigModel config, BuildOptions options)
        {
            Config = config;
            Options = options;
        }

        public BuildMethod Method {
            get {
                if (Options.Method.HasValue)
                {
                    return Options.Method.Value;
                }
                else
                {
                    return Config.Build.Method;
                }
            }
        }
    }
}