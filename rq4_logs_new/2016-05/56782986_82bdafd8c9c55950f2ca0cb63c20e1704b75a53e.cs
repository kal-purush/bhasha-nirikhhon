using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JarakTerdekat.Classes
{
    class Stopwatch
    {
        System.Diagnostics.Stopwatch watch;

        public Stopwatch()
        {
            watch = System.Diagnostics.Stopwatch.StartNew();
            watch.Stop();
        }

        public void start()
        {
            watch.Restart();
        }

        public double stop()
        {
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }
    }
}