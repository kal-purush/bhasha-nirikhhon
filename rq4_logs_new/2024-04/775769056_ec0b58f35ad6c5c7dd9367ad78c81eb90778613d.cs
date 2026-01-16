using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LagDaemon.YAMUD.Model.Utilities
{
    public class PluginDescription
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Version { get; set; }
        public string Author { get; set; }
        public string AssemblyPath { get; set; }
        public string TypeName { get; set; }
        public bool IsActive { get; set; }
    }
}