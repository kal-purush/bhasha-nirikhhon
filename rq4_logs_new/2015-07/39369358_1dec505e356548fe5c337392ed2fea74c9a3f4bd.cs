using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MARSRover.Models;

namespace MARSRover.Factory
{
    class AppMarsFactory : MarsFactory
    {
        public override IMars GetMars(int Width, int Height)
        {
            return new Mars(Width, Height);
        }
    }
}