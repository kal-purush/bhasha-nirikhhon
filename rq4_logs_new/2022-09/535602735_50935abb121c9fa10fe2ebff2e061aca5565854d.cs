using FootyTips.Models.DTOs;
using FootyTips.Services.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FootyTips.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DownloadDataController : ControllerBase
    {
        IDataDownloadService _downloadService;
        
        public DownloadDataController(IDataDownloadService dataDownloadService)
        {
            _downloadService = dataDownloadService;
        }

        [HttpGet("download")]
        public async Task<bool> DownloadDataAndUpdateData()
        {
            return await _downloadService.Download("https://www.football-data.co.uk/mmz4281/2223/E0.csv");
        }

        [HttpGet("show")]
        public async Task<List<FootballDataRAW>> ShowData()
        {
            return await _downloadService.TestFile("https://www.football-data.co.uk/mmz4281/2223/E0.csv");
        }
    }
}