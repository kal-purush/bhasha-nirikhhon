using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace UtilityLibraries
{
    public class CoolArray
    {
        private int[] a;
        Random rnd = new Random();

        public CoolArray(int lenghtArray)
        {
            a = new int[lenghtArray];
            for (int i = 0; i < lenghtArray; i++)
                a[i] = rnd.Next(1, 101);
        }

        public CoolArray(int lengthArray, int initialValue, int step)
        {
            a = new int[lengthArray];
            a[0] = initialValue;
            for (int i = 1; i < lengthArray; i++)
                a[i] = a[i - 1] + step;
        }

        public CoolArray(string filename)
        {
            //Если файл существует
            if (File.Exists(filename))
            {
                //Считываем все строки в файл 
                string[] ss = File.ReadAllLines(filename);
                a = new int[ss.Length];
                //Переводим данные из строкового формата в числовой
                for (int i = 0; i < ss.Length; i++)
                    a[i] = int.Parse(ss[i]);
            }
            else Console.WriteLine("Error load file");
        }


        public int Max
        {
            get
            {
                return a.Max();
            }
        }

        public int MaxCount
        {
            get
            {
                int count = 0;
                for (int i = 0; i < a.Length; i++)
                {
                    if (a[i] == Max)
                    {
                        count++;
                    }
                }
                return count;
            }
        }

        public int this[int i]
        {
            get { return a[i]; }
            set { a[i] = value; }
        }

        public void Print()
        {
            foreach (int el in a)
                Console.Write("{0,4}", el);
        }
        public int Length
        {
            get
            {
                return a.Length;
            }
        }

        public int Sum
        {
            get
            {
                int sum = 0;
                for (int i = 0; i < a.Length; i++)
                {
                    sum += a[i];
                }
                return sum;
            }
        }

        public int[] Inverse()
        {
            int[] b = (int[])a.Clone();
            for (int i = 0; i < a.Length; i++)
            {
                b[i] = -a[i];
            }
            return b;
        }

        public void Multi(int multi)
        {
            for (int i = 0; i < a.Length; i++)
            {
                a[i] *= multi;
            }
        }

        public Dictionary<int, int> NumberElelementsArray()
        {
            Dictionary<int, int> numberElelementsArray = new Dictionary<int, int>();
            List<int> usedNumbers = new List<int>();
            numberElelementsArray[a[0]] = 1;
            usedNumbers.Add(a[0]);
            int g;
            for (int i = 1; i < a.Length; i++)
            {
                for (g = 0; g < usedNumbers.Count; g++)
                {
                    if (a[i] == usedNumbers[g])
                    {
                        numberElelementsArray[usedNumbers[g]]++;
                        break;
                    }
                }
                if (g == usedNumbers.Count)
                {
                    usedNumbers.Add(a[i]);
                    numberElelementsArray[a[i]] = 1;
                }                
            }
            for (int i = 0; i < usedNumbers.Count; i++)
            {
                Console.WriteLine($"Число {usedNumbers[i]} встречается в массиве {numberElelementsArray[usedNumbers[i]]} раз");
            }
            return numberElelementsArray;
        }
    }
}