using Google.Cloud.Firestore;
using Microsoft.AspNetCore.Mvc;

using documents_ms.Domain.Dtos;
using documents_ms.Domain.Entities;
using documents_ms.Domain.Errors;
using documents_ms.Services;

namespace documents_ms.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ComisaryCaseController : ControllerBase
    {
        private readonly ComisaryCaseService comCaseService;
        private readonly PeopleService peopleService;

        public ComisaryCaseController(PeopleService _peopleService, ComisaryCaseService _comCaseService)
        {
            peopleService = _peopleService;
            comCaseService = _comCaseService;
        }

        private ActionResult HandleError(Exception ex)
        {
            if (ex.GetType().IsAssignableFrom(typeof(CustomError)))
            {
                var error = (CustomError)ex;
                return StatusCode(error.StatusCode, error.Message);
            }

            return StatusCode(500, "Internal Server Error");
        }

        // GET: api/ComisaryCase
        [HttpGet]
        public async Task<ActionResult<IEnumerable<ComisaryCase>>> GetDocuments()
        {
            try
            {
                var cases = await comCaseService.GetCasesAsync();
                return Ok(cases);
            }
            catch (Exception ex)
            {
                return HandleError(ex);
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ComisaryCase>> GetDocument(string id)
        {
            try
            {
                var comCase = await comCaseService.GetCaseById(id);
                return Ok(comCase);
            }
            catch (Exception ex)
            {
                return HandleError(ex);
            }
        }

        [HttpPost]
        public async Task<ActionResult<ComisaryCase>> CreateCase(CreateCaseReq req)
        {
            try
            {
                var dtoResp = CreateComisaryCaseDto.Create(req.Id, req.Title, req.PatientId, req.OffenderId);

                if (dtoResp.error != null)
                    return HandleError(CustomError.BadRequest(dtoResp.error.Message));

                var PatientId = dtoResp.Value!.PatientId;
                var OffenderId = dtoResp.Value!.OffenderId;

                if (!await peopleService.ValidatePeopleId(PatientId))
                    throw CustomError.NotFound($"Patient {PatientId} not found");

                if (!await peopleService.ValidatePeopleId(OffenderId))
                    throw CustomError.NotFound($"Offender {OffenderId} not found");

                var comCase = await comCaseService.CreateCase(dtoResp.Value!);

                return Ok(comCase);
            }
            catch (Exception ex)
            {
                return HandleError(ex);
            }
        }
    }

    public class CreateCaseReq
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = "";
        public Guid PatientId { get; set; }
        public Guid OffenderId { get; set; }
    }
}