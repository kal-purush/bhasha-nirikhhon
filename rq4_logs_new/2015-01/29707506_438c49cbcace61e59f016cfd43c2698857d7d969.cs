using System;
using System.Net;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Base_API.API
{    

    public abstract class API
    {
        private DateTime lastCall = new DateTime();
        private int statusCode = 0;
        private string returnType;
        private string requestURI = "";
        private Dictionary<string, string> headers = new Dictionary<string, string>();
        protected WebClient client = new WebClient();
        
        public API()
        {                        
        }

        public void addHeader(string name, string val)
        {
            headers.Add(name, val);
        }

        public int GetStatus { 
            get {
                return (statusCode);
            }            
        }

        public string SetURI
        {
            set
            {
                requestURI = value;
            }
        }

        public string setReturnType
        {
            set
            {
                returnType = value;
            }
        }

        public string GetRawHTMLFromURI() 
        {
            if (requestURI != "")
            {
                foreach (string key in headers.Keys)
                {
                    this.client.Headers.Add(key, headers[key]);                    
                }                
                #region Debug Request/Response Header info
                /*
                Debug.WriteLine(String.Format("Calling: {0}", requestURI));                
                Debug.WriteLine("Response Headers: ");
                for (int i = 0; i < this.client.ResponseHeaders.Count; i++)
                {
                    Debug.WriteLine(String.Format("{0}: {1}, ", this.client.ResponseHeaders.GetKey(i), this.client.ResponseHeaders[i]));
                } */
                #endregion
                Stream Data = this.client.OpenRead(this.requestURI);                
                // Set Status on Stream Read
                StreamReader reader = new StreamReader(Data);
                string http_data = reader.ReadToEnd();
                Data.Close();
                reader.Close();
                return (http_data);             
            }
            else
            {
                return ("");
            }
            
        }
    }    
}