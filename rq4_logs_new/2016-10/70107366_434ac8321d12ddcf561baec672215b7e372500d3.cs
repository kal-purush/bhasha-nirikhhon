using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;

namespace FetchURL
{
    class Program
    {
        static void Main(string[] args)
        {
            if (!FetchURL(@"http://bansky.net/feed.xml", @"D:\rss_feed.xml"))
                Console.WriteLine("Problems while fetching url");
        }

        /// <summary>
        /// Fetch given url and stores it to the file
        /// </summary>
        /// <param name="url">URL to fetch</param>
        /// <param name="fileToSave">file to store the result</param>
        /// <returns>True if operation was successful</returns>
        private static bool FetchURL(string url, string fileToSave)
        {
            bool returnStatus = true;
            try {
                WebRequest request = WebRequest.Create(url);
                // Use default system proxy
                request.Proxy = WebRequest.DefaultWebProxy;
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();

                // Check for status code
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    FileStream fs = new FileStream(fileToSave, FileMode.Create);
                    Stream responseStream = response.GetResponseStream();

                    // Read data and store them into file
                    byte[] buffer = new byte[2048];
                    int count = responseStream.Read(buffer, 0, buffer.Length);
                    while (count > 0)
                    {
                        fs.Write(buffer, 0, count);
                        count = responseStream.Read(buffer, 0, buffer.Length);
                    }

                    responseStream.Close();
                    fs.Close();
                }
                else
                    returnStatus = false;
            }
            catch {
                returnStatus = false;
            }

            // return result of the operation
            return (returnStatus);
        }
    }
}
