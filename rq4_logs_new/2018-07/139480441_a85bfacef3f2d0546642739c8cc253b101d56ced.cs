using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;

namespace DataStructuresAndLINQ
{
    public enum Endpoint
    {
        users,
        posts,
        comments,
        todos,
        address
    };

    public static class HttpRequest<T>
    {
        private const string Path = "https://5b128555d50a5c0014ef1204.mockapi.io/";

        public static List<T> GetInfo(Endpoint str)
        {
            using (var client = new HttpClient())
            {
                var response = client.GetStringAsync(Path + str).Result;
                return JsonConvert.DeserializeObject<List<T>>(response);
            }
        }

    }
}