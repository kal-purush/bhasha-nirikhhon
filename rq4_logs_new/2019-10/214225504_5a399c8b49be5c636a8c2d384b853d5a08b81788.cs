using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using EvalStr.Workers;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace EvalStr.Controllers
{
    [ApiController]
    public class EvalStrController : ControllerBase
    {
        private readonly ILogger<EvalStrController> _logger;

        public EvalStrController(ILogger<EvalStrController> logger)
        {
            _logger = logger;
        }

        [Route("evalstr")]
        [HttpGet, HttpPost]
        public async Task<HttpResponseMessage> Get()
        {
            string content = string.Empty;
            using (var reader = new StreamReader(Request.Body))
            {
                var body = await reader.ReadToEndAsync();

                _logger.LogInformation($"body: {body}");
                content = body;
            }
            if (!string.IsNullOrWhiteSpace(content))
            {
                float value;
                string errMsg;
                if (EvalStrWorker.EvalStr(content, out value, out errMsg))
                {
                    _logger.LogInformation($"value: {value}");
                    return new HttpResponseMessage
                    {
                        StatusCode = System.Net.HttpStatusCode.OK,
                        Content = new StringContent(value.ToString())
                    };
                }
                else
                {
                    _logger.LogInformation($"errMsg: {errMsg}");
                    return new HttpResponseMessage
                    {
                        StatusCode = System.Net.HttpStatusCode.BadRequest,
                        Content = new StringContent(errMsg)
                    };
                }
            }
            return new HttpResponseMessage
            {
                StatusCode = System.Net.HttpStatusCode.BadRequest,
                Content = new StringContent("Expression can't be null.")
            };
        }

        //[Route("evalstr")]
        //[HttpPost]
        //async public Task<HttpResponseMessage> Get(HttpRequestMessage httpRequest)
        //{
        //    string s = await httpRequest.Content.ReadAsStringAsync();
        //    Trace.WriteLine(s);
        //    var response = new HttpResponseMessage()
        //    {
        //        Content = new StringContent(s),
        //        RequestMessage = httpRequest
        //    };
        //    return response;
        //}
    }
}