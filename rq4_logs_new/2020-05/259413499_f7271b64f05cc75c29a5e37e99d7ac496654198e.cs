using Microsoft.VisualBasic.CompilerServices;
using System;
using System.Collections.Generic;

namespace BFS_Algorithem
{
    class BFS
    {
        public static int white = 0;
        public static int gray = 1;
        public static int black = 2;
        public static int NIL = -1;

        int[] color;
        int[] dist;
        int[] pred;
        LinkedList<int>[] list;
        int size;
        int start;

        public BFS (int numOfV, LinkedList<int>[] list)
        {
            size = numOfV;
            color = new int[size];
            this.start = 0;
            this.list = list;
            dist = new int[size];
            pred = new int[size];
        }

        public void BfsAlgorithm(int start)
        {
            for (int i=0; i<size; i++)
            {
                color[i] = white;
                dist[i] = NIL;
                pred[i] = NIL;
            }
            this.start = start;

            dist[start] = 0;
            color[start] = gray;

            Queue<int> que = new Queue<int>();
            que.Enqueue(start);

            while (que.Count > 0)
            {
                int v = que.Dequeue();
                while (list[v].Count > 0)
                {
                    //Console.WriteLine(list[v].Count);
                    var u = list[v].First;
                    list[v].RemoveFirst();
                    if (color[u.Value] == white)
                    {
                        color[u.Value] = gray;
                        dist[u.Value] = dist[v] + 1;
                        pred[u.Value] = v;
                        que.Enqueue(u.Value);
                    }
                    color[v] = black;
                }
            }
        }




        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            LinkedList<int>[] list = new LinkedList<int>[8];
            for (int i=0; i<8; i++)
            {
                list[i] = new LinkedList<int>();
            }
            list[0].AddFirst(1);
            list[0].AddFirst(4);

            list[1].AddFirst(0);
            list[1].AddFirst(5);

            list[2].AddFirst(3);
            list[2].AddFirst(5);
            list[2].AddFirst(6);

            list[3].AddFirst(2);
            list[3].AddFirst(6);
            list[3].AddFirst(7);

            list[4].AddFirst(0);

            list[5].AddFirst(1);
            list[5].AddFirst(2);
            list[5].AddFirst(6);

            list[6].AddFirst(5);
            list[6].AddFirst(2);
            list[6].AddFirst(3);
            list[6].AddFirst(7);

            list[7].AddFirst(3);
            list[7].AddFirst(6);

            BFS bfs = new BFS(list.Length, list);
            bfs.BfsAlgorithm(0);

            /*
                        (0)--------(1)        (2)--------(3)
                         |          |         /|         /|
                         |          |        / |        / |
                         |          |       /  |       /  |
                         |          |      /   |      /   |
                         |          |     /    |     /    |
                         |          |    /     |    /     |
                         |          |   /      |   /      |
                         |          |  /       |  /       |
                         |          | /        | /        |
                        (4)        (5)--------(6)--------(7)

            vertex 0 -> [1, 4]
            vertex 1 -> [0, 5]
            vertex 2 -> [3, 5, 6]
            vertex 3 -> [2, 6, 7]
            vertex 4 -> [0]
            vertex 5 -> [1, 2, 6]
            vertex 6 -> [5, 2, 3, 7]
            vertex 7 -> [3, 6]

            */

            foreach (int i in bfs.dist)
            {
                Console.WriteLine(i);
            }
        }
    }
}