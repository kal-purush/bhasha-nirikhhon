using BetterCoding.MessagePubSubCenter.Entity;
using BetterCoding.MessagePubSubCenter.Services;
using Microsoft.AspNetCore.Mvc;

namespace BetterCoding.MessagePubSubCenter.API.Controllers
{
    [ApiController]
    [Route("strapi")]
    public class StrapiWebhookController : ControllerBase
    {
        private readonly IStrapiWebhookService _strapiWebhookService;
        private readonly ILogger<StrapiWebhookController> _logger;
        public StrapiWebhookController(IStrapiWebhookService strapiWebhookService,
            ILogger<StrapiWebhookController> logger)
        {
            _strapiWebhookService = strapiWebhookService;
            _logger = logger;
        }

        [HttpPost("webhook")]
        public async Task<IActionResult> InvokeWebhook([FromBody] StrapiWebhookPayload payload)
        {
            if (payload.Event == null) return BadRequest();
            try
            {
                await _strapiWebhookService.PublishMessageAsync(payload);
                return Ok();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return StatusCode(500, new { message = "Unable to receive strapi webhook payload" });
            }
        }
    }
}