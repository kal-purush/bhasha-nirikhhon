
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
    public class mrfDetailsStatusHistoryController : ControllerBase
    {
        private readonly IUnitOfWork _unitOfWork;
        private ResponseDTO _response;
        private mrfDetailsStatusHistoryResponseModel _responseModel;
        private readonly ILoggerService _logger;
        public mrfDetailsStatusHistoryController(IUnitOfWork unitOfWork, ILoggerService logger)
        {
            _unitOfWork = unitOfWork;
            _response = new ResponseDTO();
            _responseModel = new mrfDetailsStatusHistoryResponseModel();
            _logger = logger;
        }
        // GET: api/<mrfDetailsStatusHistoryController>
        [HttpGet]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Successful response", Type = typeof(IEnumerable<mrfDetailsStatusHistory>))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public ResponseDTO Get()
        {
            _logger.LogInfo("Fetching All Gemder");
            List<mrfDetailsStatusHistory> mrfDetailsStatusHistorytList = _unitOfWork.mrfDetailsStatusHistory.GetAll().ToList();
            if (mrfDetailsStatusHistorytList.Count == 0)
            {
                _logger.LogError("No record is found");
            }
            _response.Result = mrfDetailsStatusHistorytList;
            _response.Count = mrfDetailsStatusHistorytList.Count;
            _logger.LogInfo($"Total Gemder count: {mrfDetailsStatusHistorytList.Count}");
            return _response;
        }

        // GET api/<mrfDetailsStatusHistoryController>/5
        [HttpGet("{id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Successful response", Type = typeof(mrfDetailsStatusHistory))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public ResponseDTO Get(int id)
        {
            _logger.LogInfo($"Fetching All mrfDetailsStatusHistory by Id: {id}");
            mrfDetailsStatusHistory mrfDetailsStatusHistory = _unitOfWork.mrfDetailsStatusHistory.Get(u => u.Id == id);
            if (mrfDetailsStatusHistory == null)
            {
                _logger.LogError($"No result found by this Id:{id}");
            }
            _response.Result = mrfDetailsStatusHistory;
            return _response;
        }

        // POST api/<mrfDetailsStatusHistoryController>
        [HttpPost]
        [SwaggerResponse(StatusCodes.Status201Created, Description = "Item created successfully", Type = typeof(mrfDetailsStatusHistoryResponseModel))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status422UnprocessableEntity, Description = "Unprocessable entity")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public mrfDetailsStatusHistoryResponseModel Post([FromBody] mrfDetailsStatusHistoryRequestModel request)
        {
            var mrfDetailsStatusHistory = new mrfDetailsStatusHistory
            {
                MrfId = request.MrfId,
                mrfStatusId = request.mrfStatusId,
                CreatedByEmployeeId = request.CreatedByEmployeeId,
                CreatedOnUtc = request.CreatedOnUtc,
                
            };

            _unitOfWork.mrfDetailsStatusHistory.Add(mrfDetailsStatusHistory);
            _unitOfWork.Save();

            _responseModel.Id = mrfDetailsStatusHistory.Id;
           // _responseModel.IsActive = mrfDetailsStatusHistory.IsActive;

            return _responseModel;
        }

        // PUT api/<mrfDetailsStatusHistoryController>/5
        [HttpPut("{id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Item updated successfully", Type = typeof(mrfDetailsStatusHistoryResponseModel))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status204NoContent, Description = "No content (successful update)")]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status422UnprocessableEntity, Description = "Unprocessable entity")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal server error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public mrfDetailsStatusHistoryResponseModel Put(int id, [FromBody] mrfDetailsStatusHistoryRequestModel request)
        {
            var existingStatus = _unitOfWork.mrfDetailsStatusHistory.Get(u => u.Id == id);

            if (existingStatus != null)
            {
                existingStatus.MrfId = existingStatus.MrfId;
                existingStatus.mrfStatusId = request.mrfStatusId;
                

                _unitOfWork.mrfDetailsStatusHistory.Update(existingStatus);
                _unitOfWork.Save();

                _responseModel.Id = existingStatus.Id;
                //_responseModel.IsActive = existingStatus.IsActive;
            }
            else
            {
                _logger.LogError($"No result found by this Id: {id}");
                _responseModel.Id = 0;
                _responseModel.IsActive = false;
            }
            return _responseModel;
        }

        // DELETE api/<mrfDetailsStatusHistoryController>/5
        [HttpDelete("{id}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Item deleted successfully", Type = typeof(mrfDetailsStatusHistoryResponseModel))]
        [SwaggerResponse(StatusCodes.Status204NoContent, Description = "No content (successful deletion)")]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal server error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public void Delete(int id)
        {
            mrfDetailsStatusHistory? obj = _unitOfWork.mrfDetailsStatusHistory.Get(u => u.Id == id);
            if (obj != null)
            {
                _unitOfWork.mrfDetailsStatusHistory.Remove(obj);
                _unitOfWork.Save();
            }
            else
            {
                _logger.LogError($"No result found by this Id: {id}");
            }

        }
    }
}