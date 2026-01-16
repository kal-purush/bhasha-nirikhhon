// See https://aka.ms/new-console-template for more information

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Timers;

using HolisticWare.Xamarin.Android.Bindings.Tools.NeekNoke;
using HolisticWare.Xamarin.Tools.NuGet.ServerAPI;

using Action = HolisticWare.Xamarin.Android.Bindings.Tools.NeekNoke.Action;

System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
stopwatch.Start();

string about =
@"
                 _                       _                                     
                | |                     | |                                    
 _ __   ___  ___| | ________ _ __   ___ | | _____                              
| '_ \ / _ \/ _ | |/ |______| '_ \ / _ \| |/ / _ \                             
| | | |  __|  __|   <       | | | | (_) |   |  __/                             
|_| |_|\___|\___|_|\_\      |_| |_|\___/|_|\_\___|                             
                                                                               
                                                                               
 _   _       _____      _                     _                       _        
| \ | |     |  __ \    | |                   | |                     | |       
|  \| |_   _| |  \/ ___| |_   _ __   ___  ___| | ________ _ __   ___ | | _____ 
| . ` | | | | | __ / _ | __| | '_ \ / _ \/ _ | |/ |______| '_ \ / _ \| |/ / _ \
| |\  | |_| | |_\ |  __| |_  | |_) |  __|  __|   <       | |_) | (_) |   |  __/
\_| \_/\__,_|\____/\___|\__| | .__/ \___|\___|_|\_\      | .__/ \___/|_|\_\___|
                             | |                         | |                   
                             |_|                         |_|
neek-noke
NuGet peek-poke
"
+
@"
moljac AKA miljenko mel cvjetko 
https://github.com/moljac

to collect backups

    find ./ -name ""*bckp-ts-*""
or 
    find ./ -name ""*bckp-ts-$(date +""%Y%m%d)*""

to cleanup backups:

    rm $(find ./ -name ""*bckp-ts-*"")

or

    rm $(find ./ -name ""*bckp-ts-$(date +""%Y%m%d)*"")
";

// http://easyonlineconverter.com/converters/dot-net-string-escape.html
// https://www.csharpescaper.com/
// find ./ -name "*bckp-ts-$(date +"%Y%m%d)*"

Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
Trace.AutoFlush = true;

Trace.WriteLine($"{about}");

string filename_timing = null;

foreach(string arg in args)
{
    switch (arg)
    {
        case "neek":
            NeekerNoker.Action = Action.Neek;
            break;
        case "noke":
            NeekerNoker.Action = Action.Noke;
            break;
        case string line_preprocessor when arg.StartsWith("--file-timing:"):
            filename_timing = arg.Replace("--file-timing:", "");
            break;
        default:
            Trace.WriteLine($"{arg} not recognized!!");
            Trace.WriteLine("verb/command (command line argument) can be neek or noke");
            return 1;
            break;
    }

}

string[] patterns = new string[]
                                {
                                    "*.csproj",
                                    "*.cake",
                                    "config.json",
                                    "directory.build.*.props",
                                    "directory.packages.*.props",
                                    "*.props",
                                    "*.targets",
                                    "global.json",
                                    "*.csx",
                                    "*.fsproj",
                                    "*.vbproj",
                                    "*.proj",
                                    "*.xproj",
                                    "packages.config",
                                };

Dictionary<string, string[]> patterns_files = new Scraper().Harvest(patterns);

/*
foreach (KeyValuePair<string, string[]> pattern in patterns_files)
{
    Trace.Indent();
    Trace.WriteLine(new string('=', 120));
    Trace.WriteLine($"file {pattern.Key}");

    foreach (string p in pattern.Value)
    {
        Trace.Indent();
        Trace.WriteLine($"{p}");
        Trace.Unindent();
    }

    Trace.Unindent();
}
*/

Trace.Flush();

NeekerNoker neeker_noker = new NeekerNoker();

neeker_noker.Neek(patterns_files);
neeker_noker.Dump();

Dictionary<string, string> packages_found = neeker_noker.PackageDataCleanup();

Dictionary
        <
            string, // nuget_id
            (
                string version_current,
                string version_latest,
                string[] versions_upgradeable,
                NuGetPackage package_details,
                bool failed
            )
        >
        packages_info = null;

packages_info = neeker_noker.PackageDataFetch(packages_found);

neeker_noker.Noke();
stopwatch.Stop();

// neek --file-timing:timings-System.Text.JSON.csv
// neek --file-timing:./samples/console/apps/project-references/timings-System.Text.JSON.csv
neeker_noker.DumpTiming(filename_timing, stopwatch);

return 0;