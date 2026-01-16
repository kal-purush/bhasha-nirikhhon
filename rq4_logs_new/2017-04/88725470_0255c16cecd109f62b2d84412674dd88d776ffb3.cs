using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HeatmapExtractor
{
    class Program
    {
        static void Main(string[] args)
        {
            if( args.Length != 2)
                Environment.Exit(1);

            var playerKey = args[0];
            var workingDirectory = args[1];
            if (!Directory.Exists(workingDirectory))
            {
                Console.WriteLine();
                Console.WriteLine("Error: Working directory \"" + workingDirectory + "\" does not exist.");
                Environment.Exit(1);
            }

            Console.WriteLine($"Extracting Heatmaps from {workingDirectory} for player {playerKey}");

            if (Directory.Exists(Path.Combine(workingDirectory, "Heatmaps")) == false)
                Directory.CreateDirectory(Path.Combine(workingDirectory, "Heatmaps"));

            var folders = Directory.EnumerateDirectories(workingDirectory, "Phase 2*");
            foreach (var folder in folders)
            {
                var heatmapSrcFilename = Path.Combine(workingDirectory, folder, playerKey.ToUpper(), "heatmap.png");
                var destFilename = Path.Combine(workingDirectory, "Heatmaps", "heatmap - " + folder.Remove(0, workingDirectory.Length + 1) + ".png");

                if( File.Exists(heatmapSrcFilename) == true)
                    File.Copy(heatmapSrcFilename, destFilename);
            }
#if DEBUG
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
#endif
        }
    }
}