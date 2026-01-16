using LaserAPI.Logic;
using LaserAPI.Models.Dto.Zones;
using LaserAPI.Models.FromFrontend.Zones;
using LaserAPI.Models.Helper;
using LaserAPI.Models.ToFrontend.Zones;
using Mapster;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LaserAPI.Controllers
{
    [Route("zone")]
    [ApiController]
    public class ZonesController : ControllerBase
    {
        private readonly ZoneLogic _zoneLogic;

        public ZonesController(ZoneLogic zonesLogic)
        {
            _zoneLogic = zonesLogic;
        }

        [HttpPost]
        public async Task<ActionResult> AddOrUpdate([FromBody] Zone zone)
        {
            async Task Action()
            {
                ZoneDto zoneDto = zone.Adapt<ZoneDto>();
                await _zoneLogic.AddOrUpdate(zoneDto);
            }

            var controllerErrorHandler = new ControllerErrorHandler();
            await controllerErrorHandler.Execute(Action());
            return StatusCode(controllerErrorHandler.StatusCode);
        }

        [HttpGet]
        public async Task<ActionResult<List<ZoneViewmodel>>> All()
        {
            async Task<List<ZoneViewmodel>> Action()
            {
                List<ZoneDto> zones = await _zoneLogic.All();
                return zones.Adapt<List<ZoneViewmodel>>();
            }

            var controllerErrorHandler = new ControllerErrorHandler();
            return await controllerErrorHandler.Execute(Action());
        }

        [HttpDelete("{uuid}")]
        public async Task<ActionResult> Remove(Guid uuid)
        {
            async Task Action()
            {
                await _zoneLogic.Remove(uuid);
            }

            var controllerErrorHandler = new ControllerErrorHandler();
            await controllerErrorHandler.Execute(Action());
            return StatusCode(controllerErrorHandler.StatusCode);
        }
    }
}