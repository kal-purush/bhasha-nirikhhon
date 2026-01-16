using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;

namespace PostTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Posting pst = new Posting();
            pst.POST();

            Console.ReadLine();
        }

    }

    public class Posting
    {
        public void POST()
        {
            WebRequest req = null;
            WebResponse rsp = null;
            try
            {
                string fileName = @"D:\test.xml";
                string uri = "http://localhost:8080/WebSite1/Default.aspx";
                req = WebRequest.Create(uri);
                //req.Proxy = WebProxy.GetDefaultProxy(); // Enable if using proxy
                req.Method = "POST";        // Post method
                req.ContentType = "text/xml";     // content type                
                //req
                // Wrap the request stream with a text-based writer
                StreamWriter writer = new StreamWriter(req.GetRequestStream());
                // Write the xml text into the stream
                writer.WriteLine(this.GetTextFromXMLFile(fileName));
                writer.Close();
                // Send the data to the webserver
                rsp = req.GetResponse();

                string fileToSave = @"D:\output.dat";
                FileStream fs = new FileStream(fileToSave, FileMode.Create);
                Stream responseStream = rsp.GetResponseStream();

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
            catch (WebException webEx)
            {
                Console.WriteLine(webEx.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                if (req != null) req.GetRequestStream().Close();
                if (rsp != null) rsp.GetResponseStream().Close();
            }
            
        }

        /// <summary>
        /// Read xml data from file
        /// </summary>
        /// <param name="file"></param>
        /// <returns>returns file content in xml string format</returns>
        private string GetTextFromXMLFile(string file)
        {
            StreamReader reader = new StreamReader(file);
            string ret = reader.ReadToEnd();
            reader.Close();
            return ret;
        }
    }
}