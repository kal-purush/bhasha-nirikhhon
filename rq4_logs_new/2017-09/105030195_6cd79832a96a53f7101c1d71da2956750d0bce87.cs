using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Lab11Cameron
{
    public class RegisterStudent
    {
        public string filepath = "StudentList.txt";

        public string RegisterInfo(string name, string major, int year)
        {
            if (!File.Exists(filepath))
            {
                using (FileStream fs = File.Create(filepath))
                {
                    Byte[] myText = new System.Text.UTF8Encoding(true).GetBytes($"{name}, {major}, {year}");
                    fs.Write(myText, 0, myText.Length);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.Write(Environment.NewLine);
                    sw.WriteLine($"Name: {name} \nMajor: {major} \nYear: {year}");
                }
            }
            return $"Name: {name} \nMajor: {major} \nYear: {year}";
        }

        public string ViewList()
        {
            string students = "";

            using (StreamReader sr = File.OpenText(filepath))
            {
                string[] readList = File.ReadAllLines(filepath);
                int length = readList.Length;
                foreach (string line in readList)
                {
                    students += line + "\n";
                }
            }

            return students;
        }
    }
}