using System;

namespace CW_7
{
    /// <summary>
    /// CW-7: Download file from FTP server
    /// </summary>
    public class EntryPoint
    {
        /// <summary>
        /// The entry point to the program.
        /// </summary>
        private static void Main()
        {
            try
            {
                var downloader = new FTPDownloader("ftp://ftp.pagat.com/adjogos/");
                downloader.GetFiles();
                downloader.NonParallelDownload();
                downloader.ParallelDownload();
                downloader.CompareMethods();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}