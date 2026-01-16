using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Hatfield.EnviroData.FileSystems.NetworkFileSystem
{
    public class NetworkFileSystemFilter : IFileSystemFilter
    {
        public bool Meet(object value)
        {
            return true;
        }
    }
}