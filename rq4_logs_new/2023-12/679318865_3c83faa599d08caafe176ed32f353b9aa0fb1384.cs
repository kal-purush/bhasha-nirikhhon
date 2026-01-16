using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MRF.DataAccess.Repository.IRepository;
using MRF.Models.DTO;
using MRF.Models.ViewModels;
using MRF.Utility;
using Swashbuckle.AspNetCore.Annotations;

namespace MRF.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MrfdetailsPDFController : ControllerBase
    {
        private readonly IUnitOfWork _unitOfWork;
        private ResponseDTO _response;
        private readonly ILoggerService _logger;
        private readonly IEmailService _emailService;
        private readonly IHostEnvironment _hostEnvironment;
        public MrfdetailsPDFController(IUnitOfWork unitOfWork, ILoggerService logger, IEmailService emailService, IHostEnvironment hostEnvironment)
        {
            _unitOfWork = unitOfWork;
            _response = new ResponseDTO();
            _logger = logger;
            _emailService = emailService;
            _hostEnvironment = hostEnvironment;
        }

        [HttpGet("{MrfId}")]
        [SwaggerResponse(StatusCodes.Status200OK, Description = "Successful response", Type = typeof(MrfDetailsViewModel))]
        [SwaggerResponse(StatusCodes.Status400BadRequest, Description = "Bad Request")]
        [SwaggerResponse(StatusCodes.Status401Unauthorized, Description = "Unauthorized")]
        [SwaggerResponse(StatusCodes.Status403Forbidden, Description = "Forbidden")]
        [SwaggerResponse(StatusCodes.Status404NotFound, Description = "Not Found")]
        [SwaggerResponse(StatusCodes.Status500InternalServerError, Description = "Internal Server Error")]
        [SwaggerResponse(StatusCodes.Status503ServiceUnavailable, Description = "Service Unavailable")]
        public MrfdetailsPDFRequestModel GetRequisition(int MrfId)
        {
            _logger.LogInfo($"Fetching All MRF Details by Id: {MrfId}");
            MrfdetailsPDFRequestModel mrfdetailpdf = _unitOfWork.MrfdetailsPDFRepository.GetRequisition(MrfId);
            if (mrfdetailpdf == null)
            {
                _logger.LogError($"No result found by this Id:{MrfId}");

                MrfdetailsPDFRequestModel blankData = new MrfdetailsPDFRequestModel();
                return blankData;
            }
            else
            {
                return mrfdetailpdf;
            }
        }
    }
}