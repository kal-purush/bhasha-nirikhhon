using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SPBU12._1MANAGER
{
    public abstract class Archive
    {
        //Архивация(архивирует папку с файлами с асинхронной архивацией)
        protected void DoPack(string path)
        {
            //If we got a folder
            if (IsFolder(path))
            {
                path = path.Replace("[", "").Replace("]", "");

                Directory.CreateDirectory(path + "_archived");

                DirectoryInfo di = new DirectoryInfo(path);
                FileInfo[] files = di.GetFiles();

                Parallel.ForEach(files, (currentFile) =>
                {
                    string pathFile = currentFile.FullName;
                    string compressfile = path + "_archived" + "\\" + currentFile.ToString() + ".zip";
                    CompressFile(pathFile, compressfile);
                });
            }
            //If we got a file
            else
            {              
                CompressFile(path + ".txt", Path.GetDirectoryName(path) + "\\" + Path.GetFileName(path) + ".txt" + ".zip");
            }
        }

        //Compress one file
        protected abstract void CompressFile(string pathfile, string compressfile);

        //Check if we got a folder
        private bool IsFolder(string path)
        {
            return path.Contains("[");
        }

    }

    // First realization
    class ArchiveFile : Archive
    {

        public void Pack(string path)
        {
            DoPack(path);
        }

        protected override void CompressFile(string pathfile, string compressfile)
        {
            // поток для чтения исходного файла
            using (FileStream sourceStream = new FileStream(pathfile, FileMode.OpenOrCreate))
            {
                // поток для записи сжатого файла
                using (FileStream targetStream = File.Create(compressfile))
                {
                    // поток архивации
                    using (GZipStream compressionStream = new GZipStream(targetStream, CompressionMode.Compress))
                    {
                        sourceStream.CopyTo(compressionStream); // копируем байты из одного потока в другой
                        Console.WriteLine("Сжатие файла {0} завершено. Исходный размер: {1}  сжатый размер: {2}.",
                            pathfile, sourceStream.Length.ToString(), targetStream.Length.ToString());
                    }
                }
            }
        }
    }

    // Second realization
    class ArchiveFileSecond : Archive
    {

        public void Pack(string path)
        {
            DoPack(path);
        }

        protected override void CompressFile(string pathfile, string compressfile)
        {
            Directory.CreateDirectory(pathfile + "_ZIP");
            File.Copy(pathfile, pathfile + "_ZIP" + Path.DirectorySeparatorChar + Path.GetFileName(pathfile));
            ZipFile.CreateFromDirectory(pathfile + "_ZIP", compressfile);
            Directory.Delete(pathfile + "_ZIP", true);
        }
    }
}