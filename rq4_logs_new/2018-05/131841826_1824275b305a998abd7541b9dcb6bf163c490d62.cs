using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;

namespace Reppertum.RPC
{
    public class RPCServer
    {
        public void Start() 
        {
            try
            {
                var host = new WebHostBuilder()
                    .UseKestrel()
                    .Configure(a => a.Run(ProcessAsync))
                    .Build();
                
                host.Run();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
        
        private  async Task ProcessAsync(HttpContext context)
        {
            string bodyAsString;
            
            using (var streamReader = new StreamReader(context.Request.Body, Encoding.UTF8))
            {
                bodyAsString = streamReader.ReadToEnd();
            }
            
            Console.WriteLine("Request data: " + bodyAsString);
            
            await context.Response.WriteAsync("hello");
        }
    }
}