using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JarakTerdekat
{
    public class Floyd
    {

        public List<List<double>> P;
        public List<List<double>> M;

        public double totalJarak;

        public int startIndex;
        public int endIndex;

        public List<int> result;
        public int N;


        public void init(List<List<double>> inputTable, double N)
        {
            this.N = (int)N;
            P = new List<List<double>>();
            result = new List<int>();
            M = inputTable;

            for (int i = 0; i < N; i++)
            {
                P.Add(new List<double>());

                for (int j = 0; j < N; j++)
                {
                    P[i].Add(-1);
                }
            }
        }

        public List<int> calculateShortestPath(int startIndex, int endIndex)
        {
            this.startIndex = startIndex;
            this.endIndex = endIndex;

            result.Add(startIndex);

            FloydAlgo(M);

            //Console.WriteLine("Matrix to find the shortest path of.");
            //printMatrix(M);
            //Console.WriteLine("Shortest Path Matrix.");
            //printMatrix(hasil);
            //Console.WriteLine("Path Matrix");
            //printMatrix(P);

            path(startIndex, endIndex);

            result.Add(endIndex);

            return result;
        }

        public void path(int u, int v)
        {
            double k;

            k = P[u][v];

            if (k == -1)
            {
                return;
            }
            path(u, (int)k);

            result.Add((int)k);

            path((int)k, v);
        }

        public List<List<double>> FloydAlgo(List<List<double>> M)
        {
            for (int k = 0; k < N; k++)
            {
                for (int i = 0; i < N; i++)
                {
                    for (int j = 0; j < N; j++)
                    {
                        if (M[i][k] + M[k][j] < M[i][j])
                        {
                            M[i][j] = M[i][k] + M[k][j];
                            P[i][j] = k;
                        }
                    }
                }
            }
            totalJarak = M[startIndex][endIndex];
            return M;
        }

        public int min(int i, int j)
        {
            if (i > j)
            {
                return j;
            }
            return i;
        }

        public void printMatrix(List<List<double>> Matrix)
        {
            Console.Write("\n\t");
            for (int j = 0; j < N; j++)
            {
                Console.Write(j + "\t");
            }
            Console.WriteLine();
            for (int j = 0; j < 35; j++)
            {
                Console.Write("-");
            }
            Console.WriteLine();
            for (int i = 0; i < N; i++)
            {
                Console.Write(i + " |\t");
                for (int j = 0; j < N; j++)
                {
                    Console.Write(Matrix[i][j]);
                    Console.Write("\t");
                }
                Console.Write("\n");
            }
            Console.WriteLine("\n");
        }

    }
}