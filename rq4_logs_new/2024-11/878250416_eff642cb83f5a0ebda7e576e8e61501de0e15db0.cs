using MedicalExaminationPreliminaryLists.Infrastructure.Repositories;
using MedicalExaminationPreliminaryLists.Share.DTOs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace MedicalExaminationPreliminaryLists.Api.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class DispensaryObservationsController : ControllerBase
    {
        private readonly IDispensaryObservationRepository _repository;

        public DispensaryObservationsController(IDispensaryObservationRepository repository)
        {
            _repository = repository;
        }

        [HttpGet]
        public async Task<ActionResult<List<DispensaryObservationModel>>> GetAll()
        {
            var DNs = await _repository.GetAll().ToListAsync();

            var DNsDTO = DNs.Select(dn => new DispensaryObservationModel
            {
                Number = dn.Number,
                MedProfileId = dn.MedProfileId,
                LpuType = dn.LpuType,
                DiagnosisCode = dn.DiagnosisCode,
                BeginDate = dn.BeginDate,
                EndDate = dn.EndDate,
                EndReason = dn.EndReason
            });

            return Ok(DNsDTO);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<DispensaryObservationModel>> Get(int id)
        {
            var dn = await _repository.GetByKeyAsync(id);

            if (dn == null)
            {
                return NotFound();
            }

            var dnDTO = new DispensaryObservationModel
            {
                Number = dn.Number,
                MedProfileId = dn.MedProfileId,
                LpuType = dn.LpuType,
                DiagnosisCode = dn.DiagnosisCode,
                BeginDate = dn.BeginDate,
                EndDate = dn.EndDate,
                EndReason = dn.EndReason
            };

            return Ok(dnDTO);
        }

        [HttpGet("zap/{id}")]
        public async Task<ActionResult<ZAPModel>> GetByZapId(Guid id)
        {
            var DNs = _repository.FindBy(z => z.ZAPId == id);

            var DNsDTO = DNs.Select(dn => new DispensaryObservationModel
            {
                Number = dn.Number,
                MedProfileId = dn.MedProfileId,
                LpuType = dn.LpuType,
                DiagnosisCode = dn.DiagnosisCode,
                BeginDate = dn.BeginDate,
                EndDate = dn.EndDate,
                EndReason = dn.EndReason
            });

            return Ok(DNsDTO);
        }
    }
}