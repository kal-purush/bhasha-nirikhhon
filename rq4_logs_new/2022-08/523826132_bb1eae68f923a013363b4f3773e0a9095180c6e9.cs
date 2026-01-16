using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using CassandraAPI.Storage;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace CassandraAPI.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class PetCardsController : ControllerBase
    {
        private ICardStorage storage;
        public PetCardsController(ICardStorage storage)
        {
            this.storage = storage;
        }

        // GET <PetCardsController>/pet911ru/rf123
        [EnableCors]
        [HttpGet("{ns}/{localID}")]
        public async Task<ActionResult<PetCard>> Get(string ns, string localID, [FromQuery] bool includeSensitiveData = false)
        {
            try
            {
                Trace.TraceInformation($"Getting card for {ns}/{localID}");
                var result = await this.storage.GetPetCardAsync(ns, localID);
                if (result == null)
                {
                    Trace.TraceInformation($"card for {ns}/{localID} not found");
                    return NotFound();
                }
                else
                {
                    Trace.TraceInformation($"Successfully retrieved card for {ns}/{localID}");
                    if (!includeSensitiveData)
                    {
                        Trace.TraceInformation($"Wiping out sensitive data for {ns}/{localID}");
                        var contacts = result.ContactInfo;
                        if (contacts != null) {
                            contacts.Email = new string[0];
                            contacts.Name = "";
                            contacts.Tel = new string[0];
                            contacts.Website = new string[0];
                        }
                    }
                    else
                    {
                        Trace.TraceInformation($"Returning sensitive data to the client for {ns}/{localID}");
                    }
                    return new ActionResult<PetCard>(result);
                }
            }
            catch (Exception err)
            {
                Trace.TraceError($"Except card for {ns}/{localID}: {err}");
                return StatusCode(500, err.ToString());
            }
        }

        [HttpPut("{ns}/{localID}/features/{featuresIdent}")]
        public async Task<IActionResult> PutFeatures(string ns, string localID, string featuresIdent, [FromBody] JsonPoco.FeaturesPOCO features)
        {
            try
            {
                Trace.TraceInformation($"Setting features {featuresIdent} for {ns}/{localID}");
                await this.storage.SetFeatureVectorAsync(ns, localID, featuresIdent, features.Features);
                Trace.TraceInformation($"Successfully set features {featuresIdent} for {ns}/{localID}");
                return Ok();
            }
            catch (Exception err)
            {
                Trace.TraceError($"Except card for {ns}/{localID}: {err}");
                return StatusCode(500, err.ToString());
            }
        }

        // PUT <PetCardsController>
        [HttpPut("{ns}/{localID}")]
        public async Task<ActionResult> Put(string ns, string localID, [FromBody] PetCard value)
        {
            try
            {
                Trace.TraceInformation($"Storing {ns}/{localID} into storage");
                var res = await this.storage.SetPetCardAsync(ns, localID, value);
                if (res)
                {
                    Trace.TraceInformation($"Stored {ns}/{localID}");
                    return Ok();
                }
                else
                {
                    Trace.TraceWarning($"Error while storing {ns}/{localID}");
                    return StatusCode(500);
                }
            }
            catch (Exception err)
            {
                Trace.TraceError($"Exception while storing {ns}/{localID}: {err}");
                return StatusCode(500, err.ToString());
            }

        }

        // DELETE <PetCardsController>/pet911ru/rf123
        [HttpDelete("{ns}/{localID}")]
        public async Task<ActionResult> Delete(string ns, string localID)
        {
            try
            {
                Trace.TraceInformation($"Received delete request for {ns}/{localID}");
                var res = await this.storage.DeletePetCardAsync(ns, localID);
                if (res)
                {
                    Trace.TraceInformation($"Successfully deleted {ns}/{localID} from storage");
                    return Ok();
                }
                else
                {
                    Trace.TraceError($"Failed to delete {ns}/{localID}");
                    return StatusCode(500);
                }
            }
            catch (Exception err)
            {
                Trace.TraceError($"Exception while deleting {ns}/{localID}: {err}");
                return StatusCode(500, err.ToString());
            }
        }
    }
}