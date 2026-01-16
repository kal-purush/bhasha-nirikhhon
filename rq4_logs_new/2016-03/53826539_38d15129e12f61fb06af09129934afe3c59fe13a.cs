/* XCFrameworkEngine
 * Copyright (C) Abhishek Porwal, 2016
 * Any queries? Contact author <https://github.com/abhishekp314>
 * This program is complaint with GNU General Public License, version 3.
 * For complete license, read License.txt in source root directory. */

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XCDataBuilder
{
    class Program
    {
        static string[] supportedExtensions = { ".schema" };

        static void Main(string[] args)
        {
            Console.Out.WriteLine("XCDataBuilder");
            BuildDataSchemaRecursively(args[0]);

            Console.ReadLine();
        }

        static void BuildDataSchemaRecursively(string directory)
        {
            //Call recursively for every directory
            foreach (string subDirectory in Directory.GetDirectories(directory))
            {
                BuildDataSchemaRecursively(subDirectory);
            }

            //See if any file exists
            foreach (string fileName in Directory.GetFiles(directory))
            {
                BuildDataSchema(fileName);
            }
        }

        //flatc.exe -c <filename.schema>

        static void BuildDataSchema(string filePath)
        {
            string ext = Path.GetExtension(filePath);
            string fileName = Path.GetFileNameWithoutExtension(filePath);

            if (supportedExtensions.Contains(ext))
            {
                Console.Out.WriteLine("Building " + filePath);

                StringCollection values = new StringCollection();
                string processParams = "-c " + filePath;

                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.FileName = "flatc.exe";
                startInfo.Arguments = processParams;
                startInfo.UseShellExecute = false;
                startInfo.RedirectStandardOutput = true;
                startInfo.RedirectStandardError = true;

                Process flatcProcess = Process.Start(startInfo);
                flatcProcess.OutputDataReceived += (s, e) =>
                {
                    lock (values)
                    {
                        values.Add(e.Data);
                    }
                };

                flatcProcess.ErrorDataReceived += (s, e) =>
                {
                    lock (values)
                    {
                        values.Add(e.Data);
                    }
                };

                flatcProcess.BeginErrorReadLine();
                flatcProcess.BeginOutputReadLine();
                flatcProcess.WaitForExit();

                foreach (string line in values)
                {
                    Console.Out.WriteLine(line);
                }
            }
        }
    }
}