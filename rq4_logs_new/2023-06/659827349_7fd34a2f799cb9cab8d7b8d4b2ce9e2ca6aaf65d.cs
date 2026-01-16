
using Microsoft.VisualBasic;
using System;
using System.ComponentModel;
using System.Data.SqlTypes;

namespace Learn
{
    class Program
    {
        static void Main(string[] args)
        {
            int k_1 = 6, k_2 = 5, k_3 = 4, k_4 = 4, k_5 = 4, k_6 = 3, k_7 = 2, k_8 = 2;
            int VM, F, INF, HIST, TCA, ECO, ENG, ENG_1, FIZ;
            Console.WriteLine("Enter the rating for vishmat:");
            VM = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter an assessment in physics:");
            F = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter an assessment in computer science:");
            INF = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter an estimate from history:");
            HIST = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter the TCA score:");
            TCA = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter environmental assessment");
            ECO = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter an assessment in English:");
            ENG = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter the assessment in physical education:");
            FIZ = int.Parse(Console.ReadLine());
            double rating;
            rating = 90.0 * (k_1 * VM + k_2 * F + k_3 * INF + k_4 * TCA + k_5 * HIST + k_6 * ECO + k_7 * ENG + k_8 * FIZ)
                 / ((k_1 + k_2 + k_3 + k_4 + k_5 + k_6 + k_7 + k_8) * 100);
            Console.WriteLine("Your rating score =" + rating);
            Console.ReadLine();
        }
    }
}