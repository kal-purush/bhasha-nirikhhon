using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using TextShare.Business.Interfaces;
using TextShare.Domain.Utils;

namespace TextShare.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TextFileController : ControllerBase
    {
        private readonly IPhysicalFile _physicalFile;
        private readonly IWebHostEnvironment _env;

        public TextFileController(IPhysicalFile physicalFile)
        {
            _physicalFile = physicalFile;
        }

        [HttpPost("upload")]
        public async Task<ActionResult> UploadTextFile(IFormFile file)
        {

            var res = await _physicalFile.Save(file.OpenReadStream(), file.FileName, "Images");

            return Ok(res);
        }
    }
}