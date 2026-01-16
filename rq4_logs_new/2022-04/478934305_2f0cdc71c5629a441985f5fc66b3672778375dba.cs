using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microwave.Classes.Interfaces;

namespace Microwave.Classes.Configuration
{
    public class Configuration : IConfiguration
    {
        public int MaxPower { get; set; }
    }
}