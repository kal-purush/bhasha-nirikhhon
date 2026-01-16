using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using AutoMapper;
using CourseGenerator.BLL.Interfaces;
using CourseGenerator.BLL.Infrastructure;
using CourseGenerator.BLL.DTO;

namespace CourseGenerator.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        protected readonly IMapper _mapper;
        protected readonly IUserManagementService _userManagementService;

        public AccountController(IMapper mapper, IUserManagementService userManagementService)
        {
            _mapper = mapper;
            _userManagementService = userManagementService;
        }

        [HttpPost]
        [Route("~/api/[controller]/[action]")]
        public async Task<IActionResult> Create(UserRegistrationDTO registrationDto)
        {
            OperationInfo registrationResult = await _userManagementService.CreateAsync(registrationDto);
            if (registrationResult.Succeeded)
                return StatusCode(StatusCodes.Status201Created);
            else
                return BadRequest(registrationDto);
        }
    }
}