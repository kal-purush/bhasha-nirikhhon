using System;
using System.Collections.Generic;

namespace PackageAnalyzer
{
    public class PackageBuilder
    {
        // location relative to specified rootFolder.
        const string PACKAGE_DESCRIPTIONS = "packages//descriptions";
        const string PACKAGE_CACHE = "packages//cache";
        const string PACKAGE_SOURCE = "src";
        const string PACKAGE_OUTPUT = "output";



        public List<string> Build(string start, string rootFolder)
        {
            List<string> builtPackages = new List<string>();

            return builtPackages;
        }
    }
}