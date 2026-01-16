using ComicBoxApi.App.Worker;
using Microsoft.AspNetCore.Mvc;

namespace ComicBoxApi.App
{
    [Route("api/[controller]")]
    public class ProgressController : Controller
    {
        private readonly ThumbnailWorker _thumbnailWorker;

        public ProgressController(ThumbnailWorker thumbnailWorker)
        {
            _thumbnailWorker = thumbnailWorker;
        }

        [HttpGet("thumbnailworkerstatus")]
        public ThumbnailWorkerStatus Get()
        {
            return _thumbnailWorker.GetStatus();
        }
    }
}