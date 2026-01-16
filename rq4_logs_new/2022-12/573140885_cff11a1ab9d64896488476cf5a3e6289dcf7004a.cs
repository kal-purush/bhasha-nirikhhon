using System.Xml;
using Microsoft.AspNetCore.Mvc;

using Jobs;
using Models;

namespace App.Controllers;

[ApiController]
[Route("api/[controller]")]
public class EmailStatusController : ControllerBase
{
   [HttpGet("{id}")]
    public async Task<ActionResult> GetEmailStatus(string id)
    {
        try
        {
            {
                string partnerId = "#0001";

                // TODO: 1) Authentication (JWT)

                // 2) Get Partner Data
                Partner? partner = await DBHandler.GetPartnerData(partnerId);
                if (partner is null)
                {
                    return NotFound();
                }

                // 3) Start Jobs
                Producer producer = new(partner);
                Consumer consumer = new(partner);

                // 4) Produce Payload
                List<(XmlDocument, string)> payloads = await producer.GetPayloadsToProcess();

                // 5) Process Payload
                foreach (var item in payloads)
                 {
                    (XmlDocument payload, _) = item;

                    var (response, statusCode) = await consumer.Validate(id, payload);

                    // 6) Save Response
                    // if (statusCode is not HttpStatusCode.OK)
                    // {

                    // }

                    // XmlDocument xmlResponse = new() { PreserveWhitespace = true };
                    // xmlResponse.Load(response);
                    // InvoiceResponse result = XmlHandler.DeserializeResponse<InvoiceResponse>(xmlResponse);
                }
            }

            return Ok();
        }
        catch (Exception error)
        {
            return BadRequest(error.Message);
        }
    }

    [HttpGet]
    public async Task<ActionResult> GetEmailStatus([FromQuery(Name = "year")] string? year, [FromQuery(Name = "month")] string? month)
    {
        try
        {
            {
                string partnerId = "#0001";

                // TODO: 1) Authentication (JWT)

                // 2) Get Partner Data
                Partner? partner = await DBHandler.GetPartnerData(partnerId);
                if (partner is null)
                {
                    return NotFound();
                }

                // 3) Start Jobs
                Producer producer = new(partner);
                Consumer consumer = new(partner);

                // 4) Produce Payload
                List<(XmlDocument, string)> payloads = await producer.GetPayloadsToProcess();

                // 5) Process Payload
                foreach (var item in payloads)
                {
                    (XmlDocument payload, string id) = item;

                    var (response, statusCode) = await consumer.Validate(id, payload);

                    // 6) Save Response
                    // if (statusCode is not HttpStatusCode.OK)
                    // {

                    // }

                    // XmlDocument xmlResponse = new() { PreserveWhitespace = true };
                    // xmlResponse.Load(response);
                    // InvoiceResponse result = XmlHandler.DeserializeResponse<InvoiceResponse>(xmlResponse);
                }
            }

            return Ok();
        }
        catch (Exception error)
        {
            return BadRequest(error.Message);
        }
    }

   [HttpPut("{id}")]
    public async Task<ActionResult> PutEmailStatus(string id, [FromBody] Customer customer)
    {
        try
        {
            {
                string partnerId = "#0001";

                // TODO: 1) Authentication (JWT)

                // 2) Get Partner Data
                Partner? partner = await DBHandler.GetPartnerData(partnerId);
                if (partner is null)
                {
                    return NotFound();
                }

                // 3) Start Jobs
                Producer producer = new(partner);
                Consumer consumer = new(partner);

                // 4) Produce Payload
                List<(XmlDocument, string)> payloads = await producer.GetPayloadsToProcess();

                // 5) Process Payload
                foreach (var item in payloads)
                 {
                    (XmlDocument payload, _) = item;

                    var (response, statusCode) = await consumer.Validate(id, payload);

                    // 6) Save Response
                    // if (statusCode is not HttpStatusCode.OK)
                    // {

                    // }

                    // XmlDocument xmlResponse = new() { PreserveWhitespace = true };
                    // xmlResponse.Load(response);
                    // InvoiceResponse result = XmlHandler.DeserializeResponse<InvoiceResponse>(xmlResponse);
                }
            }

            return Ok();
        }
        catch (Exception error)
        {
            return BadRequest(error.Message);
        }
    }
}