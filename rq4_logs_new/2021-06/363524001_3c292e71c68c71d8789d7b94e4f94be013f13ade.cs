using System;
using System.Threading.Tasks;
using Application.ChronicDiseases;
using Domain;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [AllowAnonymous]
    public class ChronicDiseasesController : BaseApiController
    {
        [HttpGet]
        public async Task<IActionResult> GetChronicDiseases()
        {
            return HandleResult(await Mediator.Send(new List.Query()));
        }
       

        [HttpGet("{id}")]
        public async Task<IActionResult> GetChronicDisease(Guid id)
        {
            return HandleResult(await Mediator.Send(new Details.Query{Id = id}));
        }

        [HttpPost]
        public async Task<IActionResult> CreateChronicDisease(Chronic_Disease chronic_Disease)
        {
            return HandleResult(await Mediator.Send(new Create.Command {Chronic_Disease = chronic_Disease}));
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> EditChronicDisease(Guid id, Chronic_Disease chronic_Disease)
        {
            chronic_Disease.Id = id;
            return HandleResult(await Mediator.Send(new Edit.Command{Chronic_Disease = chronic_Disease}));
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteChronicDisease(Guid id)
        {
            return HandleResult(await Mediator.Send(new Delete.Command{Id = id}));
        }
    }
}