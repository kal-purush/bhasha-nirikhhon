using SourceBuilding.Core;
using System;
using System.IO;
using System.Reflection;

namespace SqlMapper.Cmd
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var libType = LibType.Assembly;
            var executingDir = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var sourcePath = $"{executingDir}/testscaffolding/sources.txt";
            var omnisharpJson = File.ReadAllText($"{executingDir}/testscaffolding/omnisharp.json");
            var source = File.ReadAllText(sourcePath);
            var sourceBuilder = new SourceBuilder();
            var scriptBuilder = new ScriptGenerator();

            // build assemblies here
            var libraryData = sourceBuilder.BuildAssembly(new[] { source });

            var firstDbsetPropertyName = "Applications";
            var userFolder = Environment.GetEnvironmentVariable("LocalAppData");
            var appFolder = $"{userFolder}\\SqlMapper";
            var libraryPath = libType == LibType.Assembly ? $"{appFolder}\\generatedAssembly.dll" : $"{appFolder}\\generatedAssembly.csx";
            var scriptPath = $"{appFolder}\\main.csx";
            var script = scriptBuilder.BuildMainScript("GeneratedNamespace", "TempContext", libraryPath, firstDbsetPropertyName);
            var omnisharpJsonpath = $"{appFolder}\\omnisharp.json";

            var directory = new DirectoryInfo(appFolder);
            if (!directory.Exists) directory.Create();

            File.WriteAllBytes(libraryPath, libraryData);
            File.WriteAllText(scriptPath, script);
            File.WriteAllText(omnisharpJsonpath, omnisharpJson);
        }
    }
}