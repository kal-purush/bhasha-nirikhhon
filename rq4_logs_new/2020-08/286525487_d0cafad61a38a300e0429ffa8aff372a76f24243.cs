// <copyright file="CommandLineParser.cs" company="Endjin Limited">
// Copyright (c) Endjin Limited. All rights reserved.
// </copyright>

namespace Endjin.SemVer.DotNetApi.Cli
{
    using System;
    using System.CommandLine;
    using System.CommandLine.Builder;
    using System.CommandLine.Invocation;
    using System.CommandLine.Parsing;
    using System.IO;
    using System.Threading.Tasks;

    using Endjin.SemVer.DotNetApi.PackageComparison;

    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;

    public class CommandLineParser
    {
        private readonly IServiceCollection services;

        public CommandLineParser(IServiceCollection services)
        {
            this.services = services;
        }

        public delegate Task ComparePackages(IPackageCollectionComparisonOrchestrator orchestrator, ILogger logger, CompareArguments args, IConsole console, InvocationContext invocationContext = null);

        public Parser Create(ComparePackages comparePackages = null)
        {
            comparePackages ??= ComparePackagesHandler.ExecuteAsync;

            RootCommand rootCommand = Root();

            rootCommand.Handler = CommandHandler.Create<CompareArguments, InvocationContext>(async (args, context) =>
            {
                this.services.AddPackageComparison(args.Interactive)
                             .AddLogging(configure => configure.AddConsole())
                             .Configure<LoggerFilterOptions>(o => o.MinLevel = args.Verbosity);

                ServiceProvider serviceProvider = this.services.BuildServiceProvider();

                await comparePackages(
                    serviceProvider.GetRequiredService<IPackageCollectionComparisonOrchestrator>(),
                    serviceProvider.GetRequiredService<ILogger<IPackageCollectionComparisonOrchestrator>>(),
                    args,
                    context.Console,
                    context).ConfigureAwait(false);
            });

            var commandBuilder = new CommandLineBuilder(rootCommand);

            return commandBuilder.UseDefaults().Build();

            RootCommand Root()
            {
                var cmd =  new RootCommand
                {
                    Name = "nupkgversion",
                    Description = "Diff two NuGet packages and generate a Semantic Version Increment [Major | Minor | Patch].",
                };

                cmd.AddArgument(new Argument<DirectoryInfo>
                {
                    Name = "package-directory",
                    Description = "Directory where the package to evaluate is located.",
                    Arity = ArgumentArity.ExactlyOne,
                });

                cmd.AddArgument(new Argument<Uri>
                {
                    Name = "package-feed-url",
                    Description = "Uri of the package feed to compare against.",
                    Arity = ArgumentArity.ExactlyOne,
                });

                cmd.AddArgument(new Argument<bool>
                {
                    Name = "interactive",
                    Description = "Whether interactive authentication for NuGet should be used.",
                    Arity = ArgumentArity.ZeroOrOne,
                });

                cmd.AddArgument(new Argument<string>
                {
                    Name = "verbosity",
                    Description = "Logging verbosity",
                    Arity = ArgumentArity.ZeroOrOne,
                });

                return cmd;
            }

            /*Command Environment()
            {
                var cmd = new Command(
                    "compare",
                    "Manipulate the vellum-cli environment & settings.");

                this.services
                        .Configure<LoggerFilterOptions>(o => o.MinLevel = logLevel)
                        .AddSingleton<IHostedCommand, ApiCompareTool>()
                        .AddSingleton(arguments)
                        .AddPackageComparison(interactive);*/

                /*var setCmd = new Command(
                    "set",
                    "Set vellum-cli environment configuration.");

                setCmd.AddOption(new Option("--username", "Username for the current user.")
                {
                    Argument = new Argument<string>
                    {
                        Arity = ArgumentArity.ExactlyOne,
                    },
                });

                setCmd.AddOption(new Option("--workspace-path", "The location of your vellum workspace.")
                {
                    Argument = new Argument<DirectoryInfo>
                    {
                        Arity = ArgumentArity.ExactlyOne,
                    },
                });

                setCmd.AddOption(new Option("--publish-path", "The location for generated output.")
                {
                    Argument = new Argument<DirectoryInfo>
                    {
                        Arity = ArgumentArity.ExactlyOne,
                    },
                });

                setCmd.AddOption(new Option("--key", "A user-defined setting key.")
                {
                    Argument = new Argument<string>
                    {
                        Arity = ArgumentArity.ExactlyOne,
                    },
                });

                setCmd.AddOption(new Option("--value", "A user-defined setting value for the specified key.")
                {
                    Argument = new Argument<string>
                    {
                        Arity = ArgumentArity.ExactlyOne,
                    },
                });

                setCmd.AddValidator(commandResult =>
                {
                    var workspace = commandResult.ValueForOption<DirectoryInfo>("workspace-path");
                    var publish = commandResult.ValueForOption<DirectoryInfo>("publish-path");
                    var username = commandResult.ValueForOption<string>("username");
                    var key = commandResult.ValueForOption<string>("key");
                    var value = commandResult.ValueForOption<string>("value");

                    if (workspace == null && publish == null && username == null && key == null && value == null)
                    {
                        return "Please specify at least one option.";
                    }

                    if ((key != null && value == null) || (key == null && value != null))
                    {
                        return "--key & --value are mutually inclusive. Please specify a value for --key AND --value";
                    }

                    return null;
                });

                setCmd.Handler = CommandHandler.Create<SetOptions, InvocationContext>(async (options, context) =>
                {
                    await setEnvironmentSetting(options, context.Console, context).ConfigureAwait(false);
                });

                cmd.AddCommand(setCmd);

                return cmd;
            }*/
        }
    }
}