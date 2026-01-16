using System;
using System.IO;
using System.Threading;
using System.Collections.Generic;

class CopyDirectorySample
{
    static void Main()
    {
        // Copy sample
        CopyDirectory("/Users/admin/Desktop/daily report", "/Users/admin/Desktop/temp", true);
        // directory sizes
        Console.WriteLine(GetDirectorySizeInMB("/Users/admin/source"));
        string[] lines = RandomSample("/Users/admin/Desktop/daily report/first_day.txt", 3);
        foreach (string line in lines)
        {
            Console.WriteLine(line);
        }
    }

    public struct Data
    {
        public FileInfo File;
        public string Path;
        public bool Overwrite;

    }
    public static void CopyFile(object o)
    {
        Data d = (Data)o;
        d.File.CopyTo(d.Path, d.Overwrite);
    }
    public static void CopyDirectory(string sourceDirectory, string targetDirectory, bool overwriteExistingFiles)
    {
        // Get subdirectories
        DirectoryInfo dir = new DirectoryInfo(sourceDirectory);
        DirectoryInfo[] dirs = dir.GetDirectories();

        if (!dir.Exists)
        {
            throw new DirectoryNotFoundException();
        }

        // create target directory
        if (!Directory.Exists(targetDirectory))
        {
            Directory.CreateDirectory(targetDirectory);
        }

        // Get the files in the directory and copy
        FileInfo[] files = dir.GetFiles();
        foreach (FileInfo file in files)
        {
            string temp = Path.Combine(targetDirectory, file.Name);
            var data = new Data { File = file, Path = temp, Overwrite = overwriteExistingFiles };
            Thread thr = new Thread(CopyFile);
            //The input type for thread is only object.
            //Need to use structure save the parameters.
            thr.Start(data);
        }

        // copy subdirectories
        foreach (DirectoryInfo subdir in dirs)
        {
            string temppath = Path.Combine(targetDirectory, subdir.Name);
            CopyDirectory(subdir.FullName, temppath, overwriteExistingFiles);
        }

        Console.WriteLine(sourceDirectory + " Copy successfully.");
    }

    public static double GetDirectorySizeInMB(string directoryPath)
    {
        DirectoryInfo dir = new DirectoryInfo(directoryPath);
        DirectoryInfo[] dirs = dir.GetDirectories();

        long size = 0; // save size
        double result = 0.00; // save to MB

        if (!dir.Exists)
        {
            throw new DirectoryNotFoundException();
        }

        FileInfo[] files = dir.GetFiles();
        foreach (FileInfo file in files)
        {
            size += file.Length;
        }
        result = (size / 1024f) / 1024f;
        //subdirectory sizes
        foreach (DirectoryInfo subdir in dirs)
        {
            result += GetDirectorySizeInMB(subdir.FullName);
        }
        result = Math.Round(result, 2);
        //Console.WriteLine(result);
        return result;
    }

    public static string[] RandomSample(string filePath, int n)
    {
        Random rnd = new Random();
        // generate random number
        List<string> MyCollections = new List<string>();
        // save the random lines
        string[] lines = System.IO.File.ReadAllLines(filePath);
        for (int i = 0; i < n; i++)
        {
            int index = rnd.Next(1, lines.Length);
            MyCollections.Add(lines[index]);
        }
        string[] result = MyCollections.ToArray();
        return result;
    }
}