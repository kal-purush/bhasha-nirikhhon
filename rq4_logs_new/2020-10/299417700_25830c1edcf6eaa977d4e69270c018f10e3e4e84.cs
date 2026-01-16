using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using WebAdvertisements.AdvertApi.Models;
using WebAdvertisements.AdvertApi.Models.Messages;
using WebAdvertisements.AdvertApi.Responses;
using WebAdvertisements.AdvertApi.Services;

namespace WebAdvertisements.AdvertApi.Controllers
{
    [Route("api/v1/[controller]")]
    [Produces("application/json")]
    public class AdvertController : Controller
    {
        private readonly IAdvertisementStorageService _advertisementStorageService;
        private readonly IConfiguration _configuration;

        public AdvertController(IAdvertisementStorageService advertisementStorageService,
            IConfiguration configuration)
        {
            _advertisementStorageService = advertisementStorageService;
            _configuration = configuration;
        }

        [HttpPost("Create")]
        [ProducesResponseType(404)]
        [ProducesResponseType(201, Type = typeof(CreateAdvertisementResponse))]
        public async Task<IActionResult> Create([FromBody] Advertisement model)
        {
            string recordId;
            try
            {
                recordId = await _advertisementStorageService.Add(model);
            }
            catch (KeyNotFoundException)
            {
                return new NotFoundResult();
            }
            catch (Exception)
            {
                return StatusCode(500);
            }

            return StatusCode(201, new CreateAdvertisementResponse { Id = recordId });
        }

        [HttpPut("Confirm")]
        [ProducesResponseType(404)]
        [ProducesResponseType(200)]
        public async Task<IActionResult> Confirm([FromBody] ConfirmAdvertisement model)
        {
            try
            {
                await _advertisementStorageService.Confirm(model);
                await RaiseAdvertConfirmedMessage(model);
            }
            catch (KeyNotFoundException)
            {
                return new NotFoundResult();
            }
            catch (Exception)
            {
                return StatusCode(500);
            }

            return new OkResult();
        }

        [HttpGet("{id}")]
        [ProducesResponseType(404)]
        [ProducesResponseType(200)]
        public async Task<IActionResult> Get(string id)
        {
            try
            {
                var advert = await _advertisementStorageService.GetByIdAsync(id);
                return Ok(advert);
            }
            catch (KeyNotFoundException)
            {
                return new NotFoundResult();
            }
            catch (Exception)
            {
                return new StatusCodeResult(500);
            }
        }

        [HttpGet("all")]
        [ProducesResponseType(200)]
        [EnableCors("AllOrigin")]
        public async Task<IActionResult> All()
        {
            try
            {
                return Ok(await _advertisementStorageService.GetAllAsync());
            }
            catch (Exception)
            {
                return new StatusCodeResult(500);
            }
        }

        private async Task RaiseAdvertConfirmedMessage(ConfirmAdvertisement model)
        {
            var topicArn = _configuration.GetValue<string>("TopicArn");
            var dbModel = await _advertisementStorageService.GetByIdAsync(model.Id);

            using var client = new AmazonSimpleNotificationServiceClient();
            var message = new AdvertisementConfirmedMessage
            {
                Id = model.Id,
                Title = dbModel.Title
            };

            var messageJson = JsonConvert.SerializeObject(message);
            await client.PublishAsync(topicArn, messageJson);
        }
    }
}