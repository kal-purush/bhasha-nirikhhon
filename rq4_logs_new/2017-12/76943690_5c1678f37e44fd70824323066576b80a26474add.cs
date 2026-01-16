using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Advent2017.Solutions
{

    class Day18
    {
        public ConcurrentQueue<long> InputQueue = new ConcurrentQueue<long>();
        public ConcurrentQueue<long> OutputQueue = new ConcurrentQueue<long>();

        public Dictionary<string, long> Registers = new Dictionary<string, long>{{"a",0},{"b",0}};
        public long Speaker;

        public bool Waiting;
        public int SendCount;

        public long ProcessInstructions(string[] instructions)
        {
            for (long index = 0; index < instructions.Length; index++)
            {

                var x = instructions[index];
                Console.WriteLine($"{index} : {x} \t\t{Registers["a"]}\t\t{Registers["b"]}");
                var details = x.Split(' ');
                switch (details[0])
                {
                    case "snd":
                        Speaker = GetValue(details[1]);
                        break;
                    case "set":
                        Registers[details[1]] = GetValue(details[2]);
                        break;
                    case "add":
                        Registers[details[1]] = GetValue(details[1]) + GetValue(details[2]);
                        break;
                    case "mul":
                        Registers[details[1]] = GetValue(details[1]) * GetValue(details[2]);
                        break;
                    case "mod":
                        Registers[details[1]] = GetValue(details[1]) % GetValue(details[2]);
                        break;
                    case "rcv":
                        if (GetValue(details[1]) != 0) return Speaker;
                        break;
                    case "jgz":
                        if (GetValue(details[1]) > 0)
                            index = GetValue(details[2]) + index - 1;
                        break;
                    default:
                        throw new NotImplementedException($"Instruction {x} is not supported");
                }
            }

            return 0;
        }
        
        private long GetValue(string x)
        {
            if (long.TryParse(x, out long y))
            {
                return y;
            }
            if (!Registers.ContainsKey(x))
            {
                Registers[x] = 0;
            }
            return Registers[x];
        }

        public bool IsBlocked()
        {
            return Waiting;
        }

        public long ProcessInstructions2(string[] instructions, Func<bool> otherBlocked)
        {
            for (long index = 0; index < instructions.Length; index++)
            {

                var x = instructions[index];
                
                var details = x.Split(' ');
                switch (details[0])
                {
                    case "snd":
                        SendCount++;
                        OutputQueue.Enqueue(GetValue(details[1]));
                        break;
                    case "set":
                        Registers[details[1]] = GetValue(details[2]);
                        break;
                    case "add":
                        Registers[details[1]] = GetValue(details[1]) + GetValue(details[2]);
                        break;
                    case "mul":
                        Registers[details[1]] = GetValue(details[1]) * GetValue(details[2]);
                        break;
                    case "mod":
                        Registers[details[1]] = GetValue(details[1]) % GetValue(details[2]);
                        break;
                    case "rcv":
                        if (InputQueue.TryDequeue(out long result))
                        {
                            Waiting = false;
                            Registers[details[1]] = result;
                        }
                        else
                        {
                            Waiting = true;
                            index--;
                            if (otherBlocked()) return 0;
                        }
                        break;
                    case "jgz":
                        if (GetValue(details[1]) > 0)
                            index = GetValue(details[2]) + index - 1;
                        break;
                    default:
                        throw new NotImplementedException($"Instruction {x} is not supported");
                }
            }
            return 0;
        }
    }
}