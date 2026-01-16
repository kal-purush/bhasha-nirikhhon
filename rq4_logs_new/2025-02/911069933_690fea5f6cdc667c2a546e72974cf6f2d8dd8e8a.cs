using hotelier_core_app.API.Helpers;
using hotelier_core_app.Core.Constants;
using hotelier_core_app.Model.DTOs.Request;
using hotelier_core_app.Model.DTOs.Response;
using hotelier_core_app.Model.Entities;
using hotelier_core_app.Service.Interface;
using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace hotelier_core_app.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PolicyGroupsController : ControllerBase
    {
        private readonly IPolicyGroupService _policyGroupService;
        private readonly ITokenService _tokenHelper;
        private readonly IHttpContextAccessor _accessor;

        public PolicyGroupsController(IPolicyGroupService policyGroupService,
            ITokenService tokenHelper,
            IHttpContextAccessor accessor)
        {
            _policyGroupService = policyGroupService;
            _tokenHelper = tokenHelper;
            _accessor = accessor;
        }

        [HttpPost]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> AddPolicyGroup(AddPolicyGroupDTO request)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.ActivateUser,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenHelper.GetUserFullName(Request),
                IpAddress = _accessor.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenHelper.GetUserEmail(Request),
                MacAddress = _tokenHelper.GetMacAddress(Request)
            };
            BaseResponse response = await _policyGroupService.AddPolicyGroup(request, auditLog);
            return Ok(response);
        }

        [HttpGet]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> GetPolicyGroups(GetPolicyGroupsRequestDTO request)
        {
            BaseResponse response = await _policyGroupService.GetPolicyGroups(request);
            return Ok(response);
        }

        [HttpGet("{id}")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> GetSinglePolicyGroup(long id)
        {
            BaseResponse response = await _policyGroupService.GetSinglePolicyGroup(id);
            return Ok(response);
        }

        [HttpPut]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> UpdatePolicyGroup(UpdatePolicyGroupDTO request)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.ActivateUser,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenHelper.GetUserFullName(Request),
                IpAddress = _accessor.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenHelper.GetUserEmail(Request),
                MacAddress = _tokenHelper.GetMacAddress(Request)
            };
            BaseResponse response = await _policyGroupService.UpdatePolicyGroup(request, auditLog);
            return Ok(response);
        }

        [HttpPost("add-user")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> AddUserToPolicyGroup([FromQuery] long userId, [FromQuery] long policyGroupId)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.ActivateUser,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenHelper.GetUserFullName(Request),
                IpAddress = _accessor.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenHelper.GetUserEmail(Request),
                MacAddress = _tokenHelper.GetMacAddress(Request)
            };
            BaseResponse response = await _policyGroupService.AddUserToPolicyGroup(userId, policyGroupId, auditLog);
            return Ok(response);
        }

        [HttpPost("remove-user")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> RemoveUserFromPolicyGroup([FromQuery] long userId, [FromQuery] long policyGroupId)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.ActivateUser,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenHelper.GetUserFullName(Request),
                IpAddress = _accessor.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenHelper.GetUserEmail(Request),
                MacAddress = _tokenHelper.GetMacAddress(Request)
            };
            BaseResponse response = await _policyGroupService.RemoveUserFromPolicyGroup(userId, policyGroupId, auditLog);
            return Ok(response);
        }

        [HttpPost("add-permission")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> AddPermissionToPolicyGroup([FromQuery] long policyGroupId, [FromQuery] long moduleGroupId, [FromQuery] long permissionId)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.ActivateUser,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenHelper.GetUserFullName(Request),
                IpAddress = _accessor.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenHelper.GetUserEmail(Request),
                MacAddress = _tokenHelper.GetMacAddress(Request)
            };
            BaseResponse response = await _policyGroupService.AddPermissionToPolicyGroup(policyGroupId, moduleGroupId, permissionId, auditLog);
            return Ok(response);
        }

        [HttpPost("remove-permission")]
        [ProducesResponseType((int)HttpStatusCode.OK, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError, Type = typeof(BaseResponse))]
        [ProducesResponseType((int)HttpStatusCode.BadRequest, Type = typeof(ValidationResultModel))]
        public async Task<IActionResult> RemovePermissionFromPolicyGroup([FromQuery] long policyGroupId, [FromQuery] long moduleGroupId, [FromQuery] long permissionId)
        {
            AuditLog auditLog = new AuditLog
            {
                Action = UserAction.ActivateUser,
                DatePerformed = DateTime.UtcNow,
                PerformedBy = _tokenHelper.GetUserFullName(Request),
                IpAddress = _accessor.HttpContext?.Connection?.RemoteIpAddress?.ToString() ?? "Unknown IP",
                PerformerEmail = _tokenHelper.GetUserEmail(Request),
                MacAddress = _tokenHelper.GetMacAddress(Request)
            };
            BaseResponse response = await _policyGroupService.RemovePermissionFromPolicyGroup(policyGroupId, moduleGroupId, permissionId, auditLog);
            return Ok(response);
        }
    }
}