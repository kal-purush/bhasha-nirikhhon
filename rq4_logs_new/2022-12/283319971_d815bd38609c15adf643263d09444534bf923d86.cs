using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikipediaDataParser
{
    internal class CommandLineOptions
    {
        [Option('l', "language", Required = true, HelpText = "Short IETF language code.")]
        public string LanguageCode { get; set; }
        [Option('i', "in", Required = true, HelpText = "Input file with wikipedia articles data.")]
        public string SourcePath { get; set; }
        [Option('o', "out", Required = true, HelpText = "Output result file.")]
        public string OutputPath { get; set; }
        [Option('f', "format", Default = "Xml", HelpText = "Format of data file.")]
        public string FileFormat { get; set; }
    }
}