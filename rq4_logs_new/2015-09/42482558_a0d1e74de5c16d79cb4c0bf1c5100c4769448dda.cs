using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipfiles
{
    class Program
    {
        static void Main(string[] args)
        {
            var sourcefolder = "impoprtant";
            var backfolder = "backup";
            CreateZipBackup(sourcefolder, backfolder);
        }
        private static void CreateZipBackup(string folderToBackup, string destinationFolder, string filename = "backup")
        {
            if (!Directory.Exists(folderToBackup)) throw new DirectoryNotFoundException(folderToBackup + "does not exist");
            if (!Directory.Exists(destinationFolder)) Directory.CreateDirectory(destinationFolder);

            var backupName = filename + DateTime.Now.ToString("yy-MM-dd") + ".zip";
            var backupLocation = Path.Combine(destinationFolder, backupName);

            using (FileStream zipToOpen = File.Open(backupLocation, FileMode.Create))
            {
                using (ZipArchive archive = new ZipArchive(zipToOpen, ZipArchiveMode.Update))
                {
                    foreach (var file in Directory.EnumerateFiles(folderToBackup))
                    {
                        archive.CreateEntryFromFile(file, Path.GetFileName(file));
                    }
                }
            }
        }
    }
}