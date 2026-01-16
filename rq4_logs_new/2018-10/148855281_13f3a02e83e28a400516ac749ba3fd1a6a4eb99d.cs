using CommandLine;
using SymbioticTS.Core;
using System;
using System.Collections.Generic;

namespace SymbioticTS.Cli
{
    internal static class Program
    {
        /// <summary>
        /// Defines the entry point of the application.
        /// </summary>
        /// <param name="args">The arguments.</param>
        public static void Main(string[] args)
        {
            Parser.Default.ParseArguments<ProgramOptions>(args)
                   .WithParsed(Run);
        }

        private static void Run(ProgramOptions options)
        {
            SymbioticTransformer transformer = new SymbioticTransformer();

            transformer.Transform(options.InputAssemblyPath, options.OutputPath, out IReadOnlyCollection<string> filesCreated);

            Console.WriteLine($"Transformed {filesCreated.Count} objects to {options.OutputPath}:");

            foreach (string file in filesCreated)
            {
                Console.WriteLine($"    {file}");
            }
        }
    }
}