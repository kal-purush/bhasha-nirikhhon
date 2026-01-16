using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Stable.Business.Abstract.Processes;
using Stable.Business.Concrete.Requests;
using Stable.Business.Concrete.Responses.BuyingCurrencyDto;
using Stable.Core.Utilities.Results.Concrete;
using System.Linq;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;

namespace Stable.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BuyingCurrencyController : BaseController
    {
        private readonly IBuyingCurrencyProcess _buyingCurrencyProcess;

        public BuyingCurrencyController(IBuyingCurrencyProcess buyingCurrencyProcess)
        {
            _buyingCurrencyProcess = buyingCurrencyProcess;
        }

        [HttpPost]
        public async Task<DataResult<BuyingCurrencyDto>> BuyingCurrency([FromBody] BuyingCurrencyRequest buyingCurrencyRequest, CancellationToken cancellationToken)
        {
            var identity = HttpContext.User.Identity as ClaimsIdentity;
            var userId = long.Parse(identity.Claims.FirstOrDefault(u => u.Type == "userId").Value);

            buyingCurrencyRequest.UserId = userId;

            var result = await _buyingCurrencyProcess.ExecuteAsync(buyingCurrencyRequest, cancellationToken);
            return new DataResult<BuyingCurrencyDto>(Core.Utilities.Results.ComplexTypes.Enums.ResultStatus.Success, result);

        }
    }
}