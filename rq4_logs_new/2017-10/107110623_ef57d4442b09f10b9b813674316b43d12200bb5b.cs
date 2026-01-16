using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Reflection;
using System.Resources;
using System.Media;
using System.Diagnostics;

namespace DetectFileChange
{
    class Program
    {

        public static string folderPath { get; set;}
        

        [STAThread]
        static void Main(string[] args)
        {
            folderPath = "";
            
            FolderBrowserDialog folderBrowserDialog1 = new FolderBrowserDialog();
            if (folderBrowserDialog1.ShowDialog() == DialogResult.OK)
            {
                folderPath = folderBrowserDialog1.SelectedPath;
            }


            // instantiate the object
            var fileSystemWatcher = new FileSystemWatcher();

            fileSystemWatcher.IncludeSubdirectories = true;

            // Associate event handlers with the events
            fileSystemWatcher.Created += FileSystemWatcher_Created;
            fileSystemWatcher.Changed += FileSystemWatcher_Changed;
            fileSystemWatcher.Deleted += FileSystemWatcher_Deleted;
            fileSystemWatcher.Renamed += FileSystemWatcher_Renamed;

            // tell the watcher where to look
            fileSystemWatcher.Path = folderPath;

            // You must add this line - this allows events to fire.
            fileSystemWatcher.EnableRaisingEvents = true;

            Console.WriteLine("Listening to {0}...", folderPath);
            Console.WriteLine("(Press any key to exit.)");

            Console.ReadLine();
        }

        private static void FileSystemWatcher_Renamed(object sender, RenamedEventArgs e)
        {
            String timeStamp = GetTimestamp(DateTime.Now);

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("[{2}] : A new file has been renamed from {0} to {1}", e.OldName, e.Name, timeStamp);

            alertPlay("info.wav");
        }
 
        private static void FileSystemWatcher_Deleted(object sender, FileSystemEventArgs e)
        {
            String timeStamp = GetTimestamp(DateTime.Now);

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("[{1}] : A new file has been deleted - {0}", e.Name, timeStamp);

            alertPlay("delete.wav");
        }
 
        private static void FileSystemWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            
            if (!directoryCheck(folderPath + "//" + e.Name))
            {
                String timeStamp = GetTimestamp(DateTime.Now);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("[{1}] : A new file has been changed - {0}", e.Name, timeStamp);

                alertPlay("alert.wav");
            }
        }

        public static String GetTimestamp(DateTime value)
        {
            return value.ToString("HH:mm:ss");
        }

        private static void FileSystemWatcher_Created(object sender, FileSystemEventArgs e)
        {
            String timeStamp = GetTimestamp(DateTime.Now);

            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("[{1}] : A new file has been created - {0}", e.Name, timeStamp);

            alertPlay("alert.wav");
        }

        public static void alertPlay(string filename)
        {
            Assembly assembly;
            Stream soundStream;
            SoundPlayer sp;
            //Console.WriteLine("Playing {0} file", filename);
            assembly = Assembly.GetExecutingAssembly();
            sp = new SoundPlayer(assembly.GetManifestResourceStream
                (string.Format("DetectFileChange.{0}", filename)));
            sp.Play();
            
        }

        public static bool directoryCheck(string filePath)
        {
            // get the file attributes for file or directory

            FileAttributes attr = File.GetAttributes(filePath);

            //detect whether its a directory or file
            if (attr.HasFlag(FileAttributes.Directory))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}