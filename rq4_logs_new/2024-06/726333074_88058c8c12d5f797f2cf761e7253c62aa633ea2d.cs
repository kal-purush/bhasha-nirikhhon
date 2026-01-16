using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Service.Contracts;
using Shared.DataTransferObject;
using Shared.DataTransferObject.InputDto;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;

namespace EveryPinApi.Presentation.Controllers
{
    [Route("api/file-upload")]
    [ApiController]
    public class FileUploadController
    {
        private readonly ILogger _logger;
        private readonly IServiceManager _service;
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly IConfiguration _configuration;

        public FileUploadController(ILogger<FileUploadController> logger, IServiceManager service, IHttpContextAccessor httpContextAccessor, IConfiguration configuration)
        {
            _logger = logger;
            _service = service;
            _httpContextAccessor = httpContextAccessor;
            _configuration = configuration;
        }

        [HttpPost]
        [Authorize(Roles = "NormalUser")]
        public async Task<IActionResult> UploadImage([FromBody] UploadImageInputDto imageDto)
        {
            var upload = _service.UploadService.UploadTest(imageDto);

            throw new NotImplementedException();
        }
    }
}