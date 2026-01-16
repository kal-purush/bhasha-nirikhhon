using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tartuga_Simonov.EF;

namespace Tartuga_Simonov.Clases
{
    internal class Entity
    {
        public static Entities context { get; set; } = new Entities();
    }
}