using System;

namespace music
{
    class Program
    {
        static void Main(string[] args)
        {
            int[] major = new int[] { 2,2,1,2,2,2,1 };
            int[] minor = new int[] { 2, 1, 2, 2, 2, 2, 1 };
            Console.WriteLine("Enter Scale \n 1.Major \n 2.Minor");
            string scale=Console.ReadLine();
            string[] b= { "C","C#","D","D#","E","F","F#","G","G#","A","A#","B", "C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B" };
            string val = Console.ReadLine();
            int pos=0;
            int i,j;
            int[] s = new int[7];
            for (i = 0; i < 12; i++)
            {
                if (val == b[i])
                {
                    pos = i;
                }
            }
            Console.Write(b[pos]+" ");
            //Console.WriteLine(pos);

            if (scale == "1")
            {
                for (int k = 0; k < 7; k++)
                {
                    s[k] = major[k];
                    //Console.WriteLine(s[k]);
                }
            }
            else 
            {
                for (int k = 0; k < 7; k++)
                {
                    s[k] = minor[k];
                    //Console.WriteLine(s[k]);
                }
            }

           for (j = 0; j < 7; j++)
           {
                int s1 = s[j];
                Console.Write(b[pos + s[j]]+" ");
                pos = pos + s1;


            }       
        }
    }
}