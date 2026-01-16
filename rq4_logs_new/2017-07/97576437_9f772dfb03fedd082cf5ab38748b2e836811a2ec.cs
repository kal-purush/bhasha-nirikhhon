using System;
using System.IO;
using Xamarin.Forms;
using App3.Droid;
using App3.Services;

[assembly: Dependency(typeof(FileHelper))]
namespace App3.Droid
{
    public class FileHelper : IFileHelper
    {
        public string GetLocalFilePath(string filename)
        {
            string path = Environment.GetFolderPath(Environment.SpecialFolder.Personal);
            return Path.Combine(path, filename);
        }
    }
}