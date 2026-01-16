using System.Collections.Generic;
using System.Threading.Tasks;
using LernApi.Services.Blobs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.WindowsAzure.Storage.Blob;

namespace LernApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MoviesController : ControllerBase
    {
        private readonly IBlobService _blobService;

        public MoviesController(IBlobService blobService)
        {
            _blobService = blobService;
        }
        public async Task<ActionResult<HashSet<string>>> GetMovies()
        {
            return await _blobService.GetBlobs();
        }
    }
}