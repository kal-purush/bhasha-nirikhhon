using Microsoft.AspNetCore.Mvc;
using SchedulearnBackend.Controllers.DTOs;
using SchedulearnBackend.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SchedulearnBackend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AnalyzeDataController : ControllerBase
    {
        private readonly AnalyzeDataService _analyzeDataService;

        public AnalyzeDataController(AnalyzeDataService analyzeDataService)
        {
            _analyzeDataService = analyzeDataService;
        }

        // GET: api/AnalyzeData/5
        [HttpGet("{id}")]
        public async Task<ActionResult<IEnumerable<WorkerByTopicEntry>>> GetWorkersByTopic(int id)
        {
            System.Diagnostics.Debug.WriteLine($"GetWorkersByTopic {id}");
            return await _analyzeDataService.GetWorkersByTopic(id);
        }
    }
}