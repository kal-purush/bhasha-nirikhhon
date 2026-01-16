using CaptoneProject_IOTS_Repository.Base;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.RabbitMQ
{
    [Route("api/[controller]")]
    [ApiController]
    public class RabbitMQController : ControllerBase
    {
        private readonly IRabbitMQUnitOfWork _unitOfWork;

        public RabbitMQController(IRabbitMQUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        [HttpPost("send")]
        public IActionResult SendMessage([FromBody] string message)
        {
            if (string.IsNullOrEmpty(message))
            {
                return BadRequest("Message cannot be empty");
            }

            _unitOfWork.RabbitMQRepository.SendMessage(message);
            return Ok($"Message sent: {message}");
        }
    }
}