using MedicalExaminationPreliminaryLists.Data.Models;
using MedicalExaminationPreliminaryLists.Infrastructure.Common;
using MedicalExaminationPreliminaryLists.Infrastructure.Repositories;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace MedicalExaminationPreliminaryLists.Api.Application.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class MedicalExaminationPreliminaryListsController : ControllerBase
    {
        private readonly IZAPRepository _repository;

        public MedicalExaminationPreliminaryListsController(IZAPRepository repository)
        {
            _repository = repository;
        }

        [HttpPost("upload")]
        public async Task<ActionResult<List<ZAP>>> UploadFile(IFormFile file)
        {
            try
            {
                var filePath = Path.Combine(Directory.GetCurrentDirectory(), "Files", file.FileName);

                Directory.CreateDirectory(Path.GetDirectoryName(filePath));

                using (var stream = new FileStream(filePath, FileMode.Create))
                {
                    await file.CopyToAsync(stream);
                };

                return Ok(new List<ZAP>());
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
    }
}