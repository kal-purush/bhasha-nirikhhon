//Квадратное уравнение с целыми и вещественными коэффициентами.
//Возьмем уравнение типа kx^2+kx+k=0

using Automatic;

namespace Automatic
{
    class FinitAutomat
    {
        enum tCond
        {
            S  = 0,
            A = 1,
            B = 2,
            C = 3,
            D = 4,
            E = 5,
            F = 6,
            G = 7,
            H = 8,
            I = 9,
            J = 10,
            K = 11,
            L = 12,
            M = 13,
            N = 14,
            P = 15
        }
        public static bool IdLanguage (string alpha)
        {
            tCond cond = tCond.S; // cond - текущее состояние
            tCond[,] Jump = new tCond[,] 
            {
                {tCond.P, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, 
                tCond.A, tCond.A, tCond.N, tCond.P, tCond.P, tCond.P, tCond.P, tCond.C}, 
                
                {tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, 
                tCond.A, tCond.A, tCond.P, tCond.P, tCond.P, tCond.P, tCond.B, tCond.P}, 

                {tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, 
                tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.C},

                {tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P,
                tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.D, tCond.P, tCond.P},

                {tCond.P, tCond.P, tCond.E, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P,
                tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P},

                {tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P,
                tCond.P, tCond.P, tCond.F, tCond.F, tCond.P, tCond.P, tCond.P, tCond.P},

                {tCond.P, tCond.G, tCond.G, tCond.G, tCond.G, tCond.G, tCond.G, tCond.G,
                tCond.G, tCond.G, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.I},

                {tCond.G, tCond.G, tCond.G, tCond.G, tCond.G, tCond.G, tCond.G, tCond.G,
                tCond.G, tCond.G, tCond.P, tCond.P, tCond.P, tCond.P, tCond.H, tCond.P},

                {tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P,
                tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.I},

                {tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P,
                tCond.P, tCond.P, tCond.J, tCond.J, tCond.L, tCond.P, tCond.P, tCond.P},

                {tCond.P, tCond.K, tCond.K, tCond.K, tCond.K, tCond.K, tCond.K, tCond.K,
                tCond.K, tCond.K, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P},

                {tCond.K, tCond.K, tCond.K, tCond.K, tCond.K, tCond.K, tCond.K, tCond.K,
                tCond.K, tCond.K, tCond.P, tCond.P, tCond.L, tCond.P, tCond.P, tCond.P},

                {tCond.M, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P,
                tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P},

                {tCond.P, tCond.P,tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P,
                tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P},

                {tCond.P, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A, tCond.A,
                tCond.A, tCond.A, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P, tCond.P},

            };
            char[] liters = new char []{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
            '-', '+', '=', '^', '*', 'x'};
            List <char> Alph = new List<char> (liters);
            for (int i = 0; i < alpha.Length && cond != tCond.P; i ++) // просматриваем символы строки
            {
                if (Alph.Contains(alpha[i]))
                {
                    cond = Jump [(int)cond, Alph.IndexOf(alpha[i])]; // переход в новое состояние +
                }
                else
                {
                    cond = tCond.P;
                }
            }
            if (cond !=tCond.P)
                return true;
            else
                return false;
        }
    }
}

class Pogramm
{
    static void Main()
    {
        Console.WriteLine("Введите строку: ");
        string s = Console.ReadLine();
        bool res = FinitAutomat.IdLanguage(s);
            if(res)
        Console.WriteLine("Строка принята");
            else
        Console.WriteLine("Строка не принята");
    }
}