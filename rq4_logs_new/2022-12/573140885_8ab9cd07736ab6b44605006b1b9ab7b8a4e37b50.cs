using System.Xml;
using Microsoft.AspNetCore.Mvc;

using Jobs;
using Models;

namespace App.Controllers;

[ApiController]
[Route("[controller]")]
public class InvoiceController : ControllerBase
{
    [HttpGet]
    public async Task<ActionResult> GetInvoice()
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
                List<(XmlDocument, string)> payloads = await producer.GetPayloadToProcess();

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
}