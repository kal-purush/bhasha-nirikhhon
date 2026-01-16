using CaptoneProject_IOTS_BOs.DTO.PaginationDTO;
using CaptoneProject_IOTS_BOs.DTO.ProductDTO;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.LabController
{
    [Route("api/lab")]
    [ApiController]
    public class LabController : MyBaseController.MyBaseController
    {
        private readonly ILabService labService;

        public LabController(ILabService labService)
        {
            this.labService = labService;
        }

        [HttpPost("get-lab-pagination-by-combo/{comboId}")]
        public async Task<IActionResult> GetLabsPaginationByCombo(int comboId,
            [FromBody] PaginationRequest payload)
        {
            var res = await labService.GetComboLabsPagination(comboId, payload);

            return GetActionResult(res);
        }

        [HttpPost("get-lab-pagination-store-management")]
        [Authorize]
        public async Task<IActionResult> GetLabsPaginationStoreManagement([FromQuery] int? comboId,
            [FromBody] PaginationRequest payload)
        {
            var res = await labService.GetStoreManagementLabsPagination(comboId, payload);

            return GetActionResult(res);
        }

        [HttpPost("get-lab-pagination-trainer-management")]
        [Authorize]
        public async Task<IActionResult> GetLabsPaginationTrainerManagement([FromBody] GenericPaginationRequest<LabFilterRequestDTO> payload)
        {
            var res = await labService.GetTrainerManagementLabsPagination(payload.AdvancedFilter, payload.paginationRequest);

            return GetActionResult(res);
        }

        [HttpPost("get-lab-pagination-admin-manager-management")]
        [Authorize]
        public async Task<IActionResult> GetLabsPaginationAdminOrManagerManagement([FromBody] GenericPaginationRequest<LabFilterRequestDTO> payload)
        {
            var res = await labService.GetLabPagination(payload.AdvancedFilter, payload.paginationRequest);

            return GetActionResult(res);
        }

        [HttpPost("create-lab-information")]
        [Authorize]
        public async Task<IActionResult> CreateLabInformation([FromBody] CreateUpdateLabInformationDTO payload)
        {
            var res = await labService.CreateOrUpdateLabDetailsInformation(null, payload);

            return GetActionResult(res);
        }
    }
}