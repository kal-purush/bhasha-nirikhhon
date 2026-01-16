using System;
using System.Collections.Generic;
using System.IO;

namespace PackageAnalyzer.Graph.Tests
{
    public class GraphTestUtilities
    {
        
        public static GraphBuilder PopulateGB(string testcase)
        {
            GraphBuilder gb = new GraphBuilder();

            string cwd = Directory.GetCurrentDirectory();

            DirectoryInfo di = new DirectoryInfo($"{cwd}\\testcases\\{testcase}");

            FileInfo[] fis = di.GetFiles();

            foreach (FileInfo fi in fis)
            {
                gb.Insert(fi);
            }

            return gb;
        }
    }
}