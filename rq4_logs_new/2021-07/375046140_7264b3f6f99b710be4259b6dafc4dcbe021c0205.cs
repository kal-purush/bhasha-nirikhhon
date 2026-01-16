using System.Collections.Generic;
using System.Threading.Tasks;
using Api.Dtos;
using AutoMapper;
using Core.Entities.Halko;
using Core.Entities.Identity;
using Core.Interfaces;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    public class DeviceController : BaseApiController
    {
        private readonly IMapper _mapper;
        private readonly IDeviceService _deviceService;

        public DeviceController( 
            IMapper mapper,
            IDeviceService deviceService,
            UserManager<AppUser> userManager ) : base( userManager )
        {
            _mapper = mapper;
            _deviceService = deviceService;
        }

        [HttpPost]
        public async Task<ActionResult> CreateDevice(DeviceDto deviceDto)
        {
            if( !IsLogin() ) return Unauthorized();
            
            var device = _mapper.Map<Device> ( deviceDto );
            var result = await _deviceService.CreateDevice ( device );

            return result <= 0 ? BadRequest() : Ok();
        }

        [HttpGet("states")]
        public async Task<ActionResult<IReadOnlyList<DeviceState>>> GetDeviceState()
        {
            return Ok ( await _deviceService.ReadDeviceState() );
        }
    }
}