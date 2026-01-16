using System;
using CommandLine;

internal class Program
{
  internal class Options
  {
    [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
    public bool Verbose { get; set; }
  }

  public static void Main(string[] args)
  {
    Parser.Default.ParseArguments<Options>(args)
      .WithParsed<Options>(option =>
      {
        if (option.Verbose)
        {
          Console.WriteLine($"Verbose output enabled. Current Arguments: -v {option.Verbose}");
          Console.WriteLine("Quick Start Example! App is in Verbose mode!");
        }
        else
        {
          Console.WriteLine($"Current Arguments: -v {option.Verbose}");
          Console.WriteLine("Quick Start Example!");
        }
      });
  }
}