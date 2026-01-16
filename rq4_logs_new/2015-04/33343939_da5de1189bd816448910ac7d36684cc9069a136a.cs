using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using WriteOutLongTermProjectFcsts_API.Models;

namespace WriteOutLongTermProjectFcsts_API
{
    //----------------------------------------------------------------------------------------------
    // --- Source the long term project files from API on http://WETNT270:8093/api/<endpoint>
    // --- Serves as single point of availability
    //----------------------------------------------------------------------------------------------
    //
    public class Class1
    {
        public static void Main()
        {
            // clean out all folders that will contain workbooks
            Dictionary<string, string> dfp = loadStatePathDictionary();
            foreach (var k in dfp)
            {
                Array.ForEach(Directory.GetFiles(k.Value), File.Delete);
            }
            // now start download from API
            List<ProjectFcst> pfHead = getProjectListings();
            // loop through projects - select only manual forecasts
            foreach(ProjectFcst p in pfHead)
            {
               if (p.ProjManFlag.Equals("Yes"))
               {
                   getProjectFile(p.ProjID, dfp[p.State]);
                   Console.WriteLine("Project : " + p.ProjName);
               }

            }

        }
        //------------------------------------------------------------------------
        // load dictionary of file paths 
        //------------------------------------------------------------------------
        private static Dictionary<string, string> loadStatePathDictionary()
        {
            Dictionary<string, string> dcn = new Dictionary<string, string>();
            // add file paths to dictionary for each state
            dcn.Add("NSW", @"C:\Data\Projects\LongTerm\NSW\");
            dcn.Add("VIC", @"C:\Data\Projects\LongTerm\VIC\");
            dcn.Add("QLD", @"C:\Data\Projects\LongTerm\QLD\");
            dcn.Add("SA", @"C:\Data\Projects\LongTerm\SA\");
            dcn.Add("WA", @"C:\Data\Projects\LongTerm\WA\");
            dcn.Add("NZ", @"C:\Data\Projects\LongTerm\NZ\");
            //
            return dcn;
        }
        //----------------------------------------------------------------------------------------------------
        // get listing of all projects prior to downloading files - from project web api
        //----------------------------------------------------------------------------------------------------
        public static List<ProjectFcst> getProjectListings()
        {
            WebRequest req = WebRequest.Create("http://WETNT270:8093/api/projects");
            WebResponse wresp = req.GetResponse();
            Stream wstr = wresp.GetResponseStream();
            StreamReader sr = new StreamReader(wstr);
            //
            string json = sr.ReadToEnd();
            List<ProjectFcst> pfList = JsonConvert.DeserializeObject<List<ProjectFcst>>(json);
            return pfList;
        }
        //---------------------------------------------------------------------------------------------------- 
        // read file from web response
        // full file response is http://wetnt270:8093/GeneratedTemplates/<file_name>
        //----------------------------------------------------------------------------------------------------
        //
        private static void getProjectFile(int projectNo, string sFilePathToWriteFileTo)
        {
            // the path to write the file to
            //string sFilePathToWriteFileTo = @"C:\Data\";
            string sUrlToReadFileFrom = "http://WETNT270:8093/";

            WebRequest req = WebRequest.Create("http://WETNT270:8093/api/projectlines/" + projectNo.ToString().Trim());
            WebResponse response = req.GetResponse();
            Stream stream1 = response.GetResponseStream();
            StreamReader streamReader1 = new StreamReader(stream1);
            //
            string filePath = streamReader1.ReadToEnd();
            dynamic fPath = JObject.Parse(filePath);
            string fileExt = ""; string wFileExt = "";
            foreach (var g in fPath)        // enumerate the value of the jObject
            {
                wFileExt = g.Name;
                fileExt = g.Value;
            }
            sFilePathToWriteFileTo = sFilePathToWriteFileTo + wFileExt.Trim();
            response.Close();
            //
            // gets the size of the file in bytes
            Int64 iSize = response.ContentLength;
            // use the webclient object to download the file
            using (WebClient client = new WebClient())
            {
                // open the file at the remote URL for reading
                using (Stream streamRemote = client.OpenRead(new Uri(sUrlToReadFileFrom + "/" + fileExt)))
                {
                    // using the FileStream object, we can write the downloaded bytes to the file system
                    using (Stream streamLocal = new FileStream(sFilePathToWriteFileTo, FileMode.Create, FileAccess.Write, FileShare.None))
                    {
                        // loop the stream and get the file into the byte buffer
                        int iByteSize = 0;
                        byte[] byteBuffer = new byte[iSize];
                        while ((iByteSize = streamRemote.Read(byteBuffer, 0, byteBuffer.Length)) > 0)
                        {
                            // write the bytes to the file system at the file path specified
                            streamLocal.Write(byteBuffer, 0, iByteSize);
                        }
                        // clean up the file stream
                        streamLocal.Close();
                    }
                    // close the connection to the remote server
                    streamRemote.Close();
                }
            }
        }
    }
}