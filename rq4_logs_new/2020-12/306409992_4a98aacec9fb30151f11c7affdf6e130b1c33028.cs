using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace IntegrationAdapters.Controllers
{
 
       [Route("api/[controller]")]
       [ApiController]
       public class HttpController : ControllerBase
       {

           [HttpPost]
           public IActionResult SendFile()
           {
               StringBuilder builder = new StringBuilder();
               builder.Append("About medication consumption:");
               String medRep = builder.ToString();
               var test = @"C:\Users\Korisnik\Desktop\Care-CureHospital\Care-CureHospital\IntegrationAdapters\fileHttp.txt";
               System.IO.File.WriteAllText(test, medRep);
               try
               {

                   WebClient client = new WebClient();
                   // var test = @"C:\Users\Korisnik\Desktop\ra47\Care-CureHospital\Care-CureHospital\IntegrationAdapters\fileHttp.txt";
                   Uri uriString = new Uri(@"http://localhost:8080/http/fileDown");
                   client.Credentials = CredentialCache.DefaultCredentials;
                   client.UploadFile(uriString, "POST", test);
                   client.Dispose();


                   return Ok(JsonConvert.SerializeObject(test));
               }
               catch (Exception e)
               {

               }
               return Ok();
           }

        }
}