using CommandLine;
using SimpleInjector;
using System.Collections.Generic;
using System.Linq;
using TheFipster.Munchkin.Cli.Plugins.Abstractions;
using TheFipster.Munchkin.Cli.Plugins.CardStash;
using TheFipster.Munchkin.Cli.Plugins.Health;
using TheFipster.Munchkin.Cli.Plugins.Login;

namespace TheFipster.Munchkin.Cli.Client
{
    public class ArgumentCommander
    {
        private Container container;
        private string[] arguments;

        public ArgumentCommander(Container container)
        {
            this.container = container;
        }

        public int ExecuteMappedCommand(string[] args)
        {
            arguments = args;

            return Parser.Default
            .ParseArguments<
                StashVerb,
                HealthVerb,
                LoginVerb
            >(mainArgument)
            .MapResult(
                (StashVerb verb) => container.GetInstance<IPluginEntrypoint<StashVerb>>().Execute(pluginArguments),
                (HealthVerb verb) => container.GetInstance<IPluginEntrypoint<HealthVerb>>().Execute(pluginArguments),
                (LoginVerb verb) => container.GetInstance<IPluginEntrypoint<LoginVerb>>().Execute(pluginArguments),
                noMatch => 1);
        }

        private IEnumerable<string> mainArgument => arguments.Take(1);
        private IEnumerable<string> pluginArguments => arguments.Skip(1);
    }
}