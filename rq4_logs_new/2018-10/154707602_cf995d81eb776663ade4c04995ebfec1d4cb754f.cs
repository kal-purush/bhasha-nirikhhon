using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpellChecker
{
    static class FileOpener
    {
        public static void Open(string path)
        {
            File.ReadLines("HOLDROOKDAT.643");
        }
    }
}