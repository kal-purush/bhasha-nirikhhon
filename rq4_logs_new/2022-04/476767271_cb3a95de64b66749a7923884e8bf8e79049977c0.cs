using Bcp.Api.Services;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace Bcp.Api.Controllers
{
    [Route("api/blobs")]
    [ApiController]
    public class BlobExplorerController : ControllerBase
    {
        private readonly IBlobService _blobService;

        public BlobExplorerController(IBlobService blobService) 
        {
            _blobService = blobService;
        }

        [HttpGet()]
        public async Task<IActionResult> GetBlob() 
        {
            await _blobService.ListBlobAsync();
            return Ok();
        }

        [HttpGet("list")]
        public ActionResult GetBlobsList() 
        {
            return Ok();
        }
    }
}