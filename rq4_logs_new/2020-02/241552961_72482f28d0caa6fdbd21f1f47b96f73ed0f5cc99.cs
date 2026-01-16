using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace music
{
    public class Scales
    {
        public double sequence { get; set; }
        public string name { get; set; }

        public double getsequence(string n)
        {
            StreamReader r = new StreamReader("C:\\Users\\admin.INL083\\source\\repos\\music\\music\\scales.json");
            string json = r.ReadToEnd();
            List<Scales> items = JsonConvert.DeserializeObject<List<Scales>>(json);
            dynamic array = JsonConvert.DeserializeObject(json);
            //string seq;
            foreach (var item in array)
            {
                if (item.name == n)
                {
                    sequence = item.sequence;
                }                
            }
            if (sequence==0)
            {
                Console.WriteLine("Scale not available::");
                foreach (var item in array)
                {
                    Console.WriteLine(item.name);
                }
                Console.WriteLine("Enter any of these::");
                n=Console.ReadLine();
                getsequence(n);
            }
            return sequence;
            
        }

        public int[] note(double seq)
        {
            seq= Convert.ToInt32(seq);
            string seq2 = seq.ToString();
            int[] seq3 = new int[seq2.Length];
            for(int i = 0; i<seq2.Length; i++)
            {
                seq3[i] = Convert.ToInt32(seq2[i]-48);                                
            }
            //int[] seq3 = seq2.Split('').Select(n => Convert.ToInt32(n)).ToArray();
            //int[] outarry = Array.ConvertAll(seq.ToString().ToArray(), x => (int)x);
            return seq3;
        }

        public string[] show(string val,int[] note)
        {
            string[] b = { "C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B" };
            int pos = 0;
            string[] a = new string[note.Length];
            for (int i = 0; i < 12; i++)
            {
                if (val == b[i])
                {
                    pos = i;
                    for (int k = 0; k < note.Length; k++)
                    {
                        a[k] = b[pos%12];
                        pos = pos + note[k];
                        Console.Write(a[k]+" ");
                    }
                }
            }
            return a;                       
        }
    }
}