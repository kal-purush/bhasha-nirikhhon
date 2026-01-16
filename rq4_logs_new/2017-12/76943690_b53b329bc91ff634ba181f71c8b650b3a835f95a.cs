using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Advent2017.Solutions
{
    public class Day8
    {
        public Dictionary<string, int> Registers = new Dictionary<string, int>();
        public Dictionary<string, Func<int, int, bool>> Operations = new Dictionary<string, Func<int, int, bool>>
        {
            { "==", (a, b) => a == b },
            { "!=", (a, b) => a != b },
            { ">" , (a, b) => a >  b },
            { ">=", (a, b) => a >= b },
            { "<" , (a, b) => a <  b },
            { "<=", (a, b) => a <= b }
        };
        public int HighWaterMark { get; set; }

        public void ProcessInstructions(string[] input)
        {
            Registers = new HashSet<string>(
                input.Select(x => x.Split(' ')[0])
                .Union(input.Select(x => x.Split(' ')[4])))
                .ToDictionary(x => x, x => 0);
            

            foreach (var line in input)
            {
                var parts = line.Split(' ');
                var register = parts[0];
                var instruction = parts[1];
                var amount = Convert.ToInt32(parts[2]);
                var conditionRegister = parts[4];
                var condition = parts[5];
                var comparison = Convert.ToInt32(parts[6]);

                if (!Operations[condition](Registers[conditionRegister], comparison)) continue;
                if (instruction == "inc")
                {
                    Registers[register] += amount;
                }
                else
                {
                    Registers[register] -= amount;
                }

                if (Registers[register] > HighWaterMark)
                {
                    HighWaterMark = Registers[register];
                }
            }
        }
    }
}