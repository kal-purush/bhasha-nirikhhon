using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AlgoritmGenetic
{
    public static class AG
    {
        public static Graph THEgraph;

        public static void Innit()
        {
            THEgraph = new Graph(@"../../GrafDemo.txt");
        }
    }
}