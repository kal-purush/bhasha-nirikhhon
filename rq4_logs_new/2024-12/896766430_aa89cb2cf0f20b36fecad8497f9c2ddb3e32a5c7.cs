using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Schema;

namespace AoC2024
{
    internal class Day10 : AoC2024
    {
        public override void Solve()
        {
            string[] puzzleInput = this.ReadPuzzleInput("C:\\projects\\AoC2024\\input\\Dag10.txt");
            //string[] puzzleInput = this.ReadPuzzleInput("C:\\Users\\pnl14s5t\\source\\repos\\AoC2024\\input\\Day10_test.txt");

            int[,] playMap = new int[puzzleInput.Length, puzzleInput[0].Length];
            for (int i = 0; i < puzzleInput.Length; i++)
            {
                for (int j = 0; j < puzzleInput[0].Length; j++)
                {
                    playMap[i, j] = puzzleInput[i][j] - '0';
                }
                
            }
            Console.WriteLine(playMap);
            LinkedList<Route> routes = new LinkedList<Route>();
            for (int i = 0; i < puzzleInput.Length; i++)
            {
                for (int j = 0; j < puzzleInput[0].Length; j++) {
                    if (playMap[i, j] == 0)
                    {
                        routes.AddLast(new Route(i, j, 0, playMap));
                    }
                }
                    
            }
            HashSet<Coord> found_endsteps = new HashSet<Coord>();
            LinkedList<Coord> found_endsteps_all = new LinkedList<Coord>();
            int total = 0;
            int totalAll = 0;
            foreach(Route startingPoint in routes)
            {
                LinkedList<Route> nextPoint = new LinkedList<Route>();
                nextPoint.AddFirst(startingPoint);
                while(nextPoint.First != null)
                {
                    LinkedList<int[]> nextSteps = nextPoint.First.Value.next_step(nextPoint.First.Value.i, nextPoint.First.Value.j, nextPoint.First.Value.currentValue);
                    foreach (int[] step in nextSteps)
                    {
                        if (step[2] == 9)
                        {
                            found_endsteps.Add(new Coord(step[0], step[1]));
                            found_endsteps_all.AddLast(new Coord(step[0], step[1]));
                        }
                        else
                        {
                            nextPoint.AddLast(new Route(step[0], step[1], step[2], playMap));
                        }
                    }
                    nextSteps.Clear();
                    nextPoint.RemoveFirst();
                }
                total += found_endsteps.Count();
                totalAll += found_endsteps_all.Count();
                found_endsteps.Clear();
                found_endsteps_all.Clear();
            }
            foreach(Coord end in found_endsteps)
            {
                Console.Write(end.I);
                Console.Write(", ");
                Console.WriteLine(end.J);
            }
            Console.WriteLine(total);
            Console.WriteLine(totalAll);
            Console.ReadLine();
        }
    }
    internal class Route
    {
        public int i;
        public int j;
        public int currentValue;
        int[,] playMap;
        HashSet<Coord> foundTrails = new HashSet<Coord>();
        LinkedList<Route> active_options = new LinkedList<Route>();

        public Route(int i, int j, int currentValue, int[,] playMap)
        {
            this.i = i;
            this.j = j;
            this.currentValue = currentValue;
            this.playMap = playMap;
        }
        
        public LinkedList<int[]> next_step(int i, int j, int currentValue)
        {
            LinkedList<int[]> response = new LinkedList<int[]> { };
            if (i > 0 && playMap[i - 1, j] == currentValue + 1)
            {
                response.AddLast(new int[] { i - 1, j, currentValue + 1 });
            }
            if (i < playMap.GetLength(0)-1 && playMap[i + 1, j] == currentValue + 1)
            {
                response.AddLast(new int[] { i + 1, j, currentValue + 1 });
            }
            if (j > 0 && playMap[i, j - 1] == currentValue + 1)
            {
                response.AddLast(new int[] { i, j - 1, currentValue + 1 });
            }
            if (j < playMap.GetLength(1)-1 && playMap[i, j + 1] == currentValue + 1)
            {
                response.AddLast(new int[] { i, j +1, currentValue + 1 });
            }
            return response;
        }

    }

}