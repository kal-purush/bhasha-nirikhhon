using MedicalExaminationPreliminaryLists.Data.Models;
using MedicalExaminationPreliminaryLists.Infrastructure.Repositories;
using MedicalExaminationPreliminaryLists.Share.DTOs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace MedicalExaminationPreliminaryLists.Api.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class UploadFilesController : ControllerBase
    {
        private readonly IUploadFileRepository _repository;

        public UploadFilesController(IUploadFileRepository repository)
        {
            _repository = repository;
        }

        [HttpGet]
        public async Task<ActionResult<List<UploadFileModel>>> GetAll()
        {
            var files = await _repository.GetAll().ToListAsync();

            var dictionariesDTO = files.Select(f => new UploadFileModel
            {
                Id = f.Id,
                FileName = f.FileName,
                UploadDate = f.UploadDate
            });

            return Ok(dictionariesDTO);
        }
    }
}