using System;

namespace Excercise5
{
    class Program
    {
        static void Main(string[] args)
        {
            String[] classes = new String[] {"English III","Precalculus", "Music Theory", "Biotechnology", "Principles of Technology I", "Latin II", "AP US History", "Business Computer Infomation Systems", "Magic"};
            String[] teachers = new String[] {"Ms.Lapan","Mrs.Gideon","Mr.Davis","Ms.Palmer","Ms.Garcia","Mrs.Barnett","Ms.Johannessen","Mr.James","Albus Dambledore"};
            Console.WriteLine("+-------------------------------------------------------------+");
            
            for (int i = 0; i < classes.Length; i++)
            {
                String line = "";
                line = String.Join(line,"| ",(i+1).ToString()," |");
                for(int j = 0; j < 38-classes[i].Length; j++) line += " ";
                line += classes[i]+"|";
                for (int j = 0; j < 18-teachers[i].Length; j++) line += " ";
                line += teachers[i] + "|";
                Console.WriteLine(line);
            }

            Console.WriteLine("+-------------------------------------------------------------+");
            Console.ReadKey();
        }
    }
}