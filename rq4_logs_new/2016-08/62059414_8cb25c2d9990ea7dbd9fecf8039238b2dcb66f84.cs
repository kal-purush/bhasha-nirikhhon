using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetNumFrames
{
    class Program
    {
        [STAThread]
        public static void Main(string[] args)
        {
            string[] supportedTypes = { ".mp4", ".mov", ".avi" };
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: GetNumFrames filepath");
                return;
            }
            if(Array.IndexOf(supportedTypes, Path.GetExtension(args[0]).ToLower()) == -1)
            {
                Console.WriteLine("Usage: GetNumFrames filepath (type must be supported: {0})", supportedTypes.ToString());
                return;
            }
            Console.OutputEncoding = Encoding.UTF8;

            List<string> arrHeaders = new List<string>();
            List<int> arrIndeces = new List<int>();

            Shell32.Shell shell = new Shell32.Shell();
            Shell32.Folder objFolder;

            objFolder = shell.NameSpace(Path.GetDirectoryName(args[0]));


            for (int i = 0; i < short.MaxValue; i++)
            {
                string header = objFolder.GetDetailsOf(null, i);
                //if (String.IsNullOrEmpty(header))
                //    break;
                //arrHeaders.Add(header);
                if (!String.IsNullOrEmpty(header))
                {
                    arrHeaders.Add(header);
                    arrIndeces.Add(i);
                }
            }

            //foreach (Shell32.FolderItem2 item in objFolder.Items())
            Shell32.FolderItem item = objFolder.ParseName(Path.GetFileName((args[0])));
            if(true)
            {
                string lenString = "";
                int frameRate = 0;
                int secs = 0;
                int i = 0;
                for (i = 0; i < arrHeaders.Count; i++)
                {
                    //Console.WriteLine("{0}\t{1}: {2}", arrIndeces[i], arrHeaders[i], objFolder.GetDetailsOf(item, arrIndeces[i]));
                    if (arrIndeces[i] == 27) // length
                    {
                        lenString = objFolder.GetDetailsOf(item, arrIndeces[i]);
                        string[] lenTokens = lenString.Split(':');
                        secs = int.Parse(lenTokens[2]) + (60 * int.Parse(lenTokens[1])) + (60 * 60 * int.Parse(lenTokens[0]));
                    }
                    else if (arrIndeces[i] == 305) //frame rate
                    {
                        string fr = objFolder.GetDetailsOf(item, arrIndeces[i]);
                        string fr2 = fr.Substring(1);
                        int j = 0;
                        for (j = 0; j < fr2.Length; j++)
                        {
                            if ((fr2[j] < '0') || (fr2[j] > '9'))
                                break;
                        }
                        int val = 0;
                        int.TryParse(fr.Substring(1,j), out val);
                        frameRate = val;
                        //Console.WriteLine("frame rate {0}", val);
                    }
                }
                int frames = 0;
                switch (frameRate)
                {
                    case 29:
                        frames = (int)((float)secs * 29.97);
                        break;
                    case 59:
                        frames = (int)((float)secs * 59.94);
                        break;
                    default:
                        frames = (int)((float)secs * frameRate);
                        break;
                }
                Console.WriteLine("{0}", frames);
                //Console.WriteLine("{0}\t{1}: {2}", i, arrHeaders[i], objFolder.GetDetailsOf(item, 281));
            }
        }
    }
}