using System;
using System.Net;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;

namespace Stream_Browser.API
{
    class Threads
    {
        // Create a semaphore that can satisfy up to three 
        // concurrent requests. Use an initial count of three so that the
        // threads can start immediately without needing to be released.            
        private static Semaphore _pool = new Semaphore(3, 3);
        private static int _padding;

        private static void Worker(object num)
        {
            // Each Worker thread begins by requesting the semaphore            
            _pool.WaitOne();
            // A padding interval to make the output more orderly
            int padding = Interlocked.Add(ref _padding, 100);            
            // The threads's "work" consits of sleeping for about a second
            // Each thread "words" a little longer, just to make the output
            // More orderly.
            Thread.Sleep(1000 + padding);            
        }

        public string CallHttp(string url, Dictionary<string, string> headers = null)
        {
            WebClient client = new WebClient();
            Thread t = new Thread(new ParameterizedThreadStart(Worker));            

            if (url != "")
            {
                #region Header processing
                if (headers != null)
                {
                    foreach (string key in headers.Keys)
                    {
                        client.Headers.Add(key, headers[key]);
                    }
                }
                #endregion

                #region Debug Request/Response Header info
                /*
                Debug.WriteLine(String.Format("Calling: {0}", requestURI));                
                Debug.WriteLine("Response Headers: ");
                for (int i = 0; i < this.client.ResponseHeaders.Count; i++)
                {
                    Debug.WriteLine(String.Format("{0}: {1}, ", this.client.ResponseHeaders.GetKey(i), this.client.ResponseHeaders[i]));
                } */
                #endregion

                try
                {
                    Stream Data = client.OpenRead(url);
                    StreamReader reader = new StreamReader(Data);
                    string http_data = reader.ReadToEnd();
                    Data.Close();
                    reader.Close();
                    return (http_data);
                }
                catch (WebException e)
                {
                    Debug.WriteLine(
                        string.Format("WebException: {0}", e.Message)
                        );
                    throw new ArgumentNullException();
                }
            }
            else
            {
                return ("");
            }

        }
    }
}
