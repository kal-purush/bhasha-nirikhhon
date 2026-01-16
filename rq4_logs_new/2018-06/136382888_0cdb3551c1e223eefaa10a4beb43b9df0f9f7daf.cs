using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.IO;
using JRequest.Net;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string json = ReadJsonFile("json1.json");
                var jRequest = JRequestEngine.Run(json);

                Console.WriteLine($"JRequest Name: {jRequest.Name}");
                foreach (var request in jRequest.Requests)
                {
                    Console.WriteLine($"Request Key: {request.Key}");
                    Console.WriteLine($"Response Status: {request.Response.Status}");
                    Console.WriteLine("Response Content:");
                    Console.WriteLine(request.Response.Content);
                    Console.WriteLine("--------------------------------------------------------------------------------");
                    Console.WriteLine(" ");
                }
                
                Console.Read();
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public static string ReadJsonFile(string JsonfilePath)
        {
            try
            {
                string json = string.Empty;
                using (StreamReader r = new StreamReader(JsonfilePath))
                {
                    json = r.ReadToEnd();

                }
                return json;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
    }
}