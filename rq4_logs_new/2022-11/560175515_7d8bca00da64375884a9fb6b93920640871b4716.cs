using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IELTSpeaking
{
    class ExaminerImage
    {
        private static readonly string _imagesDirectory = CurrentDirectory.Directory + "\\Images\\";

        //private static List<string> _photosNames = Directory.GetFiles(_imagesDirectory).Select(file => Path.GetFileName(file)).ToList();
        //private static string[] _photosDirectories = _photosNames.Select(str => WriteDirectory(str)).ToArray();

        //static private string WriteDirectory(string filename)
        //{
        //    return _imagesDirectory + filename;
        //}

        private static readonly List<string> _photosDirectories = Directory.EnumerateFiles(_imagesDirectory).ToList();

        public Image LoadImage()
        {
            Random random = new Random();
            int rndm = random.Next(_photosDirectories.Count);
            
            return Image.FromFile(_photosDirectories[rndm]);
        }
    }
}