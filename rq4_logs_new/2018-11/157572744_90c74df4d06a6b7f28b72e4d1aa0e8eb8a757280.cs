using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Win32;

namespace DSAAlgorythm.Data
{
    public static class FileSaver
    {
        public static void SaveTextToFile(string text)
        {
            SaveFileDialog saveFileDialog = new SaveFileDialog();
            saveFileDialog.Filter = "Text File|*.txt";
            saveFileDialog.Title = $"{text}";
            saveFileDialog.ShowDialog();
            if (!File.Exists(saveFileDialog.FileName))
            {
                System.IO.FileStream fs = (System.IO.FileStream)saveFileDialog.OpenFile();
                
            }
            //switch (saveFileDialog.FilterIndex)
            //{
            //    case 1:
            //        File.Create(saveFileDialog.FileName);
            //        TextWriter tw = new StreamWriter(saveFileDialog.FileName);
            //        tw.WriteLine(text);
            //        break;
            //}
        }
    }
}