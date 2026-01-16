using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipelineSimulation.Core
{
    public class Memory
    {
        public static Memory Instance;
        public byte[] MemorySpace { get; set; } = new byte[1048576]; //1 MiB = 1024 KiB = 1024 * 1024 B

        public static Memory GetInstance()
        {
            if (Instance == null)
                Instance = new Memory();
            return Instance;
        }
    }
}