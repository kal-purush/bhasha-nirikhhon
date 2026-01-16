using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System.Threading;
using System.Globalization;


namespace Company
{
    public static class CreateUser
    {

      [Function("CreateUser")]
      public static async Task<IActionResult> CreateUserWithUserKey(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "create-user")] HttpRequest req,
          ILogger log)
      {
        throw new NotImplementedException();
      }
    }
}