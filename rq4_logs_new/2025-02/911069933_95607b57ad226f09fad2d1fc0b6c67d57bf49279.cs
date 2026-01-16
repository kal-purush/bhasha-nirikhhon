using hotelier_core_app.API.Helpers;
using hotelier_core_app.Core.Constants;
using hotelier_core_app.Model.DTOs.Request;
using hotelier_core_app.Model.DTOs.Response;
using hotelier_core_app.Model.Entities;
using hotelier_core_app.Service.Interface;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace hotelier_core_app.API.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    [Authorize]
    public class ModuleController : ControllerBase
    {
        private readonly IModuleService _moduleService;
        private readonly ITokenService _tokenService;
        private readonly IHttpContextAccessor _accessor;

        public ModuleController(
            IModuleService moduleService,
            ITokenService tokenService,
            IHttpContextAccessor accessor)
        {
            _moduleService = moduleService;
            _tokenService = tokenService;
            _accessor = accessor;
        }

        [HttpPost("module-groups")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> CreateModuleGroup(CreateModuleGroupDTO model)
        {

            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.CreateModuleGroup,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenService.GetUserFullName(Request),
                IpAddress = _accessor?.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenService.GetUserEmail(Request),
                PerformedAgainst = "Module Group",
                MacAddress = _tokenService.GetMacAddress(Request)
            };
            BaseResponse response = await _moduleService.CreateModuleGroup(model, auditLog);
            return Ok(response);
        }

        [HttpPut("module-groups/{id}")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> EditModuleGroup(long id, EditModuleGroupDTO model)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.EditModuleGroup,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenService.GetUserFullName(Request),
                IpAddress = _accessor?.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenService.GetUserEmail(Request),
                PerformedAgainst = "Module Group",
                MacAddress = _tokenService.GetMacAddress(Request)
            };
            BaseResponse response = await _moduleService.EditModuleGroup(id, model, auditLog);
            return Ok(response);
        }

        [HttpDelete("module-groups/{id}")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> DeleteModuleGroup(long id)
        {

            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.EditModuleGroup,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenService.GetUserFullName(Request),
                IpAddress = _accessor?.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenService.GetUserEmail(Request),
                PerformedAgainst = "Module Group",
                MacAddress = _tokenService.GetMacAddress(Request)
            };
            BaseResponse response = await _moduleService.DeleteModuleGroup(id, auditLog);
            return Ok(response);
        }

        [HttpGet("module-groups")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse<List<ModuleGroupDTO>>))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> GetAllModuleGroup()
        {
            BaseResponse<List<ModuleGroupDTO>> response = await _moduleService.GetAllModuleGroup();
            return Ok(response);
        }

        [HttpPost]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> CreateModule(CreateModuleDTO model)
        {

            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.CreateModule,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenService.GetUserFullName(Request),
                IpAddress = _accessor?.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenService.GetUserEmail(Request),
                PerformedAgainst = "Module",
                MacAddress = _tokenService.GetMacAddress(Request)
            };
            BaseResponse response = await _moduleService.CreateModule(model, auditLog);
            return Ok(response);
        }

        [HttpPut("{id}")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> EditModule(long id, EditModuleDTO model)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.EditModule,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenService.GetUserFullName(Request),
                IpAddress = _accessor?.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenService.GetUserEmail(Request),
                PerformedAgainst = "Module",
                MacAddress = _tokenService.GetMacAddress(Request)
            };
            BaseResponse response = await _moduleService.EditModule(id, model, auditLog);
            return Ok(response);
        }

        [HttpDelete("{id}")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> DeleteModule(long id)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.EditModuleGroup,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenService.GetUserFullName(Request),
                IpAddress = _accessor?.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenService.GetUserEmail(Request),
                PerformedAgainst = "Module Group",
                MacAddress = _tokenService.GetMacAddress(Request)
            };
            BaseResponse response = await _moduleService.DeleteModule(id, auditLog);
            return Ok(response);
        }

        [HttpGet]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse<List<ModuleDTO>>))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> GetAllModule()
        {
            BaseResponse<List<ModuleDTO>> response = await _moduleService.GetAllModule();
            return Ok(response);
        }
    }
}