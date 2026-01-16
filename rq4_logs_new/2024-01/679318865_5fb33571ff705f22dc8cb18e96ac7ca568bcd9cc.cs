using Microsoft.AspNetCore.Mvc;
using MRF.DataAccess.Repository.IRepository;
using MRF.Models.DTO;
using MRF.Models.Models;
using MRF.Utility;
using Swashbuckle.AspNetCore.Annotations;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace MRF.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class InterviewevaluationHistoryController : ControllerBase
    {
        private readonly IUnitOfWork _unitOfWork;
        private ResponseDTO _response;
        private InterviewevaluationHistoryResponseModel _responseModel;
        private readonly ILoggerService _logger;
        public InterviewevaluationHistoryController(IUnitOfWork unitOfWork, ILoggerService logger)
        {
            _unitOfWork = unitOfWork;
            _response = new ResponseDTO();
            _responseModel = new InterviewevaluationHistoryResponseModel();
            _logger = logger;
        }
        // GET: api/<InterviewevaluationHistoryResponseModel>
        [HttpGet]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Successful response", Type = typeof(IEnumerable<InterviewevaluationHistory>))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public ResponseDTO Get()
        {
            _logger.LogInfo("Fetching All Candidate Status");
            List<InterviewevaluationHistory> InterviewevaluationList = _unitOfWork.InterviewevaluationHistory.GetAll().ToList();
            if (InterviewevaluationList == null)
            {
                _logger.LogError("No record is found");
            }
            _response.Result = InterviewevaluationList;
            _logger.LogInfo($"Total candidate status count: {InterviewevaluationList.Count}");
            return _response;
        }

        // GET api/<InterviewevaluationHistoryResponseModel>/5
        [HttpGet("{Id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Successful response", Type = typeof(InterviewevaluationHistory))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public ResponseDTO Get(int Id)
        {

            _logger.LogInfo($"Fetching All Candidate Status by Id: {Id}");
            InterviewevaluationHistory interviewevaluation = _unitOfWork.InterviewevaluationHistory.Get(u => u.Id == Id);
            if (interviewevaluation == null)
            {
                _logger.LogError($"No result found by this Id: {Id}");
            }
            _response.Result = interviewevaluation;
            return _response;
        }

        // POST api/<InterviewevaluationHistorynController>
        [HttpPost]
        [SwaggerResponse(StatusCodes.Status201Created, Description = "Item created successfully", Type = typeof(InterviewevaluationHistoryResponseModel))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status422UnprocessableEntity, Description = "Unprocessable entity")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public InterviewevaluationHistoryResponseModel Post([FromBody] InterviewevaluationHistoryRequestModel request)

        {
            var interviewevaluation = new InterviewevaluationHistory();

            if (!string.IsNullOrEmpty(request.interviewerEmployeeIds))
            {

                List<InterviewevaluationHistory>? obj = _unitOfWork.InterviewevaluationHistory.GetCandidateByCandidateid(request.CandidateId);
                foreach (InterviewevaluationHistory inter in obj)
                {
                    _unitOfWork.InterviewevaluationHistory.Remove(inter);
                    _unitOfWork.Save();
                }

                var employeeIds = request.interviewerEmployeeIds.Split(',');
                foreach (var employeeId in employeeIds)
                {
                    var interviewevaluation1 = new Interviewevaluation();
                    interviewevaluation1.InterviewerId = int.Parse(employeeId);
                    interviewevaluation1.CandidateId = request.CandidateId;
                    interviewevaluation1.EvalutionStatusId = request.EvalutionStatusId == 0 ? null : request.EvalutionStatusId;
                    interviewevaluation1.EvaluationDateUtc = request.EvaluationDateUtc;
                    interviewevaluation1.FromTimeUtc = request.FromTimeUtc;
                    interviewevaluation1.ToTimeUtc = request.ToTimeUtc;
                    interviewevaluation1.CreatedByEmployeeId = request.CreatedByEmployeeId;
                    interviewevaluation1.CreatedOnUtc = request.CreatedOnUtc;
                    interviewevaluation1.UpdatedByEmployeeId = request.UpdatedByEmployeeId;
                    interviewevaluation1.UpdatedOnUtc = request.UpdatedOnUtc;
                    _unitOfWork.Interviewevaluation.Add(interviewevaluation1);
                    _unitOfWork.Save();
                }
            }

            else
            {

                interviewevaluation.InterviewerId = request.InterviewerId;
                interviewevaluation.CandidateId = request.CandidateId;
                interviewevaluation.EvaluationDateUtc = request.EvaluationDateUtc;
                interviewevaluation.FromTimeUtc = request.FromTimeUtc;
                interviewevaluation.EvalutionStatusId = request.EvalutionStatusId;
                interviewevaluation.EvaluationDateUtc = request.EvaluationDateUtc;
                interviewevaluation.FromTimeUtc = request.FromTimeUtc;
                interviewevaluation.ToTimeUtc = request.ToTimeUtc;
                interviewevaluation.EvalutionStatusId = request.EvalutionStatusId;
                interviewevaluation.CreatedByEmployeeId = request.CreatedByEmployeeId;
                interviewevaluation.CreatedOnUtc = request.CreatedOnUtc;
                interviewevaluation.UpdatedByEmployeeId = request.UpdatedByEmployeeId;
                interviewevaluation.UpdatedOnUtc = request.UpdatedOnUtc;
                _unitOfWork.InterviewevaluationHistory.Add(interviewevaluation);
                _unitOfWork.Save();

            }
            _responseModel.Id = interviewevaluation.Id;
            return _responseModel;
        }
        // PUT api/<InterviewevaluationHistoryController>/5
        [HttpPut("{id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Item updated successfully", Type = typeof(InterviewevaluationHistoryResponseModel))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status204NoContent, Description = "No content (successful update)")]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status422UnprocessableEntity, Description = "Unprocessable entity")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal server error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public InterviewevaluationHistoryResponseModel Put(int id, [FromBody] InterviewevaluationHistoryRequestModel request)
        {

            List<InterviewevaluationHistory> record = _unitOfWork.InterviewevaluationHistory.GetA(u => u.CandidateId == request.CandidateId).ToList();

            if (record.Count > 0)
            {
                for (int i = 0; i < record.Count; i++)
                {

                    var existingRecord = record[i];
                    if (existingRecord != null)
                    {
                        existingRecord.CandidateId = request.CandidateId == 0 ? existingRecord.CandidateId : request.CandidateId;
                        existingRecord.InterviewerId = request.InterviewerId == 0 ? existingRecord.InterviewerId : request.InterviewerId;
                        existingRecord.EvaluationDateUtc = request.EvaluationDateUtc == DateOnly.MinValue ? existingRecord.EvaluationDateUtc : request.EvaluationDateUtc;
                        existingRecord.FromTimeUtc = request.FromTimeUtc == TimeOnly.MinValue ? existingRecord.FromTimeUtc : request.FromTimeUtc;
                        existingRecord.ToTimeUtc = request.ToTimeUtc == TimeOnly.MinValue ? existingRecord.ToTimeUtc : request.ToTimeUtc;
                        existingRecord.EvalutionStatusId = request.EvalutionStatusId == 0 ? existingRecord.EvalutionStatusId : request.EvalutionStatusId;
                        existingRecord.UpdatedByEmployeeId = request.UpdatedByEmployeeId;
                        existingRecord.UpdatedOnUtc = request.UpdatedOnUtc;


                        _unitOfWork.InterviewevaluationHistory.Update(existingRecord);
                        _unitOfWork.Save();

                        _responseModel.Id = existingRecord.Id;
                    }
                    else
                    {
                        _logger.LogError($"No result found by this Id: {id}");

                        _responseModel.Id = 0;
                        _responseModel.Status = null;
                        _responseModel.IsActive = false;
                    }
                }
            }
            else
            {
                Post(request);
            }

            return _responseModel;

        }

        // DELETE api/<InterviewevaluationHistoryController>/5
        [HttpDelete("{Id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Item deleted successfully", Type = typeof(InterviewevaluationHistoryResponseModel))]
        [SwaggerResponse(StatusCodes.Status204NoContent, Description = "No content (successful deletion)")]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal server error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public void Delete(int Id)
        {
             InterviewevaluationHistory? obj = _unitOfWork.InterviewevaluationHistory.Get(u => u.Id == Id);
            if (obj == null)
            {
                _logger.LogError($"No result found by this Id: {Id}");
            }
            _unitOfWork.InterviewevaluationHistory.Remove(obj);
            _unitOfWork.Save();
        }
    }
}