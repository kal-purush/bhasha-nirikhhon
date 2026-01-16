using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace StringProblems
{
    class GIFFilesNamesList
    {

        // Read the line 
        //store the data in class log file structure 
        static void StoreGIFFilenamesinFile(string[] args)
        {
            List<string> giflist = new List<string>();
            string fname = "hosts_access_log_"; //string.Format("{0:00}
            for (int i = 0; i < 14; i++)
            {
                var filecount = string.Format("{0:00}", i++);

                // read the string filename
                string filename = Console.ReadLine();
                string line;
                //using (var sr = new StreamReader("hosts_access_log_00.txt"))
                using (var sr = new StreamReader(fname + filecount + ".txt"))
                {


                    while ((line = sr.ReadLine()) != null)
                    {
                        if (line.ToLower().Contains(".gif") && line.Contains("200"))
                        {

                            string s1 = line.Substring(line.IndexOf("\""));
                            string gifnamepath = s1.Substring(0, s1.LastIndexOf("\""));
                            string[] status = s1.Substring(s1.LastIndexOf("\"")).Split(' ');
                            string gifrequest = gifnamepath.Substring(0, gifnamepath.LastIndexOf(".gif") + 4);
                            string giffilename = gifrequest.Substring(gifrequest.LastIndexOf("/") + 1);
                            //Console.WriteLine(giffilename);
                            if (status[1] == "200")
                            {
                                if (!giflist.Contains(giffilename))
                                {
                                    giflist.Add(giffilename);
                                }
                            }
                        }
                    }
                }
            }
        

            using (TextWriter tw = new StreamWriter("gifs_" + "TestFileName"))
            {
                foreach (string fn in giflist)
                {
                    tw.WriteLine(fn);
                }
            }
        }
    }
}
