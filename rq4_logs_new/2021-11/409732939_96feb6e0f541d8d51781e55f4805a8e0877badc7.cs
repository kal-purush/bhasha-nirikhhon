using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ardalis.ApiEndpoints;
using Contracts.Requests;
using Microsoft.AspNetCore.Mvc;
using Services.Abstractions;

namespace Presentation.Endpoints
{
    [Route("api")]
    public class TestRequestHandler : BaseEndpoint
        .WithRequest<TestRequest>
        .WithResponse<string>
    {
        private readonly IServiceManager _serviceManager;
        public TestRequestHandler(IServiceManager serviceManager) => _serviceManager = serviceManager;

        [HttpPost("TestRequest")]
        public override ActionResult<string> Handle([FromBody] TestRequest request) =>
            _serviceManager.TestService.DoRequestTest(request);
    }
}