using Microsoft.AspNetCore.Mvc;
using MRF.DataAccess.Repository;
using MRF.DataAccess.Repository.IRepository;
using MRF.Models.DTO;
using MRF.Models.Models;
using MRF.Utility;
using Swashbuckle.AspNetCore.Annotations;
using System.Xml.Linq;

namespace MRF.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CandidateInterviewFeedbackController : ControllerBase
    {
        private readonly IUnitOfWork _unitOfWork;
        private ResponseDTO _response;
        private CandidateInterviewFeedbackResponseModel _responseModel;
        private readonly ILoggerService _logger;
        public CandidateInterviewFeedbackController(IUnitOfWork unitOfWork, ILoggerService logger)
        {
            _unitOfWork = unitOfWork;
            _response = new ResponseDTO();
            _responseModel = new CandidateInterviewFeedbackResponseModel();
            _logger = logger;
        }

        [HttpGet]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Successful response", Type = typeof(IEnumerable<CandidateInterviewFeedback>))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public ResponseDTO Get()
        {
            _logger.LogInfo("Fetching All Gemder");
            List<CandidateInterviewFeedback> candidateInterviewFeedbackList = _unitOfWork.CandidateInterviewFeedback.GetAll().ToList();
            if (candidateInterviewFeedbackList.Count == 0)
            {
                _logger.LogError("No record is found");
            }
            _response.Result = candidateInterviewFeedbackList;
            _response.Count = candidateInterviewFeedbackList.Count;
            _logger.LogInfo($"Total Gemder count: {candidateInterviewFeedbackList.Count}");
            return _response;
        }

        // GET api/<CandidateInterviewFeedbackController>/5
        [HttpGet("{id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Successful response", Type = typeof(CandidateInterviewFeedback))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public ResponseDTO Get(int id)
        {
            _logger.LogInfo($"Fetching All Gender by Id: {id}");
            CandidateInterviewFeedback candidateInterviewFeedback = _unitOfWork.CandidateInterviewFeedback.Get(u => u.Id == id);
            if (candidateInterviewFeedback == null)
            {
                _logger.LogError($"No result found by this Id:{id}");
            }
            _response.Result = candidateInterviewFeedback;
            return _response;
        }


        // POST api/<CandidateInterviewFeedbackController>
        [HttpPost]
        [SwaggerResponse(StatusCodes.Status201Created, Description = "Item created successfully", Type = typeof(CandidateInterviewFeedbackResponseModel))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status422UnprocessableEntity, Description = "Unprocessable entity")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public CandidateInterviewFeedbackResponseModel Post([FromBody] CandidateInterviewFeedbackRequestModel request)
        {
            var candidateInterviewFeedback = new CandidateInterviewFeedback
            {
                CandidateId = request.CandidateId,
                SoftSkills = request.SoftSkills,
                HardSkills = request.HardSkills,
                RequiredTraining = request.RequiredTraining,
                Comments = request.Comments,
                CreatedByEmployeeId = request.CreatedByEmployeeId,
                CreatedOnUtc = request.CreatedOnUtc,
                UpdatedByEmployeeId = request.UpdatedByEmployeeId,
                UpdatedOnUtc = request.UpdatedOnUtc
            };

            _unitOfWork.CandidateInterviewFeedback.Add(candidateInterviewFeedback);
            _unitOfWork.Save();

            _responseModel.Id = candidateInterviewFeedback.Id;           

            return _responseModel;
        }

        // PUT api/<CandidateInterviewFeedbackController>/5
        [HttpPut("{id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Item updated successfully", Type = typeof(CandidateInterviewFeedbackResponseModel))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status204NoContent, Description = "No content (successful update)")]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status422UnprocessableEntity, Description = "Unprocessable entity")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal server error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public CandidateInterviewFeedbackResponseModel Put(int id, [FromBody] CandidateInterviewFeedbackRequestModel request)
        {
            var existingStatus = _unitOfWork.CandidateInterviewFeedback.Get(u => u.Id == id);

            if (existingStatus != null)
            {
                existingStatus.CandidateId = request.CandidateId;
                existingStatus.SoftSkills = request.SoftSkills;
                existingStatus.HardSkills = request.HardSkills;
                existingStatus.RequiredTraining = request.RequiredTraining;
                existingStatus.Comments = request.Comments;
                existingStatus.CreatedByEmployeeId = request.CreatedByEmployeeId;
                existingStatus.CreatedOnUtc = request.CreatedOnUtc;
                existingStatus.UpdatedByEmployeeId = request.UpdatedByEmployeeId;
                existingStatus.UpdatedOnUtc = request.UpdatedOnUtc;
                
                _unitOfWork.CandidateInterviewFeedback.Update(existingStatus);
                _unitOfWork.Save();

                _responseModel.Id = existingStatus.Id;             
            }
            else
            {
                _logger.LogError($"No result found by this Id: {id}");
                _responseModel.Id = 0;         
            }
            return _responseModel;
        }

        // DELETE api/<CandidateInterviewFeedbackController>/5
        [HttpDelete("{id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Item deleted successfully", Type = typeof(CandidateInterviewFeedbackResponseModel))]
        [SwaggerResponse(StatusCodes.Status204NoContent, Description = "No content (successful deletion)")]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal server error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public void Delete(int id)
        {
            CandidateInterviewFeedback? obj = _unitOfWork.CandidateInterviewFeedback.Get(u => u.Id == id);
            if (obj != null)
            {
                _unitOfWork.CandidateInterviewFeedback.Remove(obj);
                _unitOfWork.Save();
            }
            else
            {
                _logger.LogError($"No result found by this Id: {id}");
            }

        }
    }
}