using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Stable.Business.Abstract.Processes;
using Stable.Business.Concrete.Requests;
using Stable.Business.Concrete.Responses.GetMyTransactionDto;
using Stable.Core.Utilities.Results.Concrete;
using System.Linq;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;

namespace Stable.API.Controllers
{
    public class GetMyTransactionsController : BaseController
    {
        private readonly IGetMyTransactionProcess _getMyTransactionProcess;

        public GetMyTransactionsController(IGetMyTransactionProcess getMyTransactionProcess)
        {
            _getMyTransactionProcess = getMyTransactionProcess;
        }

        [HttpGet("getMyTransaction")]
        public async Task<DataResult<GetMyTransactionDto>> GetMyTransaction(CancellationToken cancellationToken)
        {
            var identity = HttpContext.User.Identity as ClaimsIdentity;
            var userId = long.Parse(identity.Claims.FirstOrDefault(u => u.Type == "userId").Value);

            var getMyTransactionRequest = new GetMyTransactionRequest();
            getMyTransactionRequest.UserId = userId;

            var result = await _getMyTransactionProcess.ExecuteAsync(getMyTransactionRequest, cancellationToken);

            return new DataResult<GetMyTransactionDto>(Core.Utilities.Results.ComplexTypes.Enums.ResultStatus.Success, result);
        }
    }
}