using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using exercise2.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Retrofit.Net;

namespace exercise2
{
    class Program
    {
        static void Main(string[] args)
        {
            var url = "https://jsonplaceholder.typicode.com";
            var service = GetPostService(url);
            // display all post
            var posts = service.GetPost().Data;
            posts.ForEach(Console.WriteLine);
            // display post at position 5
            var position = 5;
            var post = service.GetPost(position).Data;
            Console.WriteLine(post);
            Console.ReadKey();
        }

        private static IPost GetPostService(string url)
        {
            var adapter = new RestAdapter(url);
            var service = adapter.Create<IPost>();
            return service;
        }
    }
}