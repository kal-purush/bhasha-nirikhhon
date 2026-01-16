using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PackageAnalyzer;

namespace ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {

            string packageroot = "A";
            string packagedescriptions = @"\pkgs\descriptions";   
            string packagesource = @"\src";    
            string testroot = @"C:\tmp";
 
            GraphBuilder gb = new GraphBuilder($"{testroot}{packagedescriptions}");

            if (gb.Graph.IsCyclic)
            {
                Console.WriteLine("Build graph contains cyclic dependencies.");
                return;
            }

            DirectoryInfo di = new DirectoryInfo($"{testroot}{packagesource}");
            List<string> paths = di.GetDirectories().Select(d => d.FullName).ToList();
            var task = Task.Run(async () => await PackageHasher.HashFoldersAsync(paths, testroot));
            Dictionary<string, string> packageSourceHash = task.Result;
            
            PackageBuilder pb = new PackageBuilder(testroot);
            Dictionary<string, bool> buildResults = pb.Build(packageroot, $"{testroot}{packagesource}", packageSourceHash, gb.Graph);

            foreach (string key in buildResults.Keys)
            {
                Console.WriteLine($"package {key} build : {buildResults[key]}");
            }

        }
    }
}