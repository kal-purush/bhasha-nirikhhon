using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using VNNSIS.Core.Interfaces;

namespace VNNSIS.BackendApi.Controllers
{
     public class DefectiveController : BaseApiController
     {
          private readonly IDefectiveService _service;
          public DefectiveController(IDefectiveService service)
          {
               _service = service;
          }

          [HttpGet("{job}")]
          public async Task<ActionResult> GetData(string job)
          {
               var result = await _service.GetDataByJob(job);
               return Ok(result);
          }
     }
}