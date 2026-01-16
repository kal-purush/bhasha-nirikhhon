using Microsoft.AspNetCore.Mvc;
using Bcp.Domain.Dtos;
using System.Threading.Tasks;
using Bcp.Api.Services.Common;
// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Bcp.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BinChangeController : ControllerBase
    {
        private readonly ICommonService _commonService;
        public BinChangeController(ICommonService commonService)
        {
            _commonService = commonService;
        }

        [HttpPost("SaveBinChanges")]
        public async Task<IActionResult> SaveBinChanges(StockWmsDto binChanges)
        {
           await _commonService.SaveBinChanges(binChanges);
            return Ok();
        }
    }
}