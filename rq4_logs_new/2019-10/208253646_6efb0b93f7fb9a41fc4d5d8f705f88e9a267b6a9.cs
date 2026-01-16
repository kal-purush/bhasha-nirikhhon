using CommandLine;
using System.Collections.Generic;
using TheFipster.Munchkin.Cli.HealthPlugin.Commands;
using TheFipster.Munchkin.Cli.HealthPlugin.Verbs;
using TheFipster.Munchkin.Cli.Plugins.Abstractions;

namespace TheFipster.Munchkin.Cli.HealthPlugin
{
    public class HealthCommand : IPluginEntrypoint
    {
        public int Execute(IEnumerable<string> args) => Parser.Default
            .ParseArguments<
                CheckVerb,
                EndpointsVerb
            >(args)
            .MapResult(
                (CheckVerb opts) => new CheckCommand().Execute(opts),
                (EndpointsVerb opts) => new EndpointsCommand().Execute(opts),
                noMatch => 1);
    }
}