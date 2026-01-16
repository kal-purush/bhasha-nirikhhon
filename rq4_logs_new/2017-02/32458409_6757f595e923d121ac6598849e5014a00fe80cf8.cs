using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DDTE.Common
{
    public class GeneralHelper
    {
        public static string GetTumbPath(string path) 
        {
            if (String.IsNullOrWhiteSpace(path))
                return path;

            var extPos = path.LastIndexOf('.');
            if (extPos < 0)
                return path;

            return path.Substring(0, extPos) + ".tmb" + path.Substring(extPos);
        }

        public static bool IsLocalPath(string path)
        {
            return !(path ?? String.Empty).StartsWith("http");
        }
    }
}