using System;
using System.Globalization;
using System.Threading.Tasks;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using raBudget.Application.Features.Transactions.Command;
using raBudget.Application.Features.Transactions.Query;
using raBudget.Application.Features.TransactionTemplates.Command;
using raBudget.Application.Features.TransactionTemplates.Query;

namespace raBudget.Api.ApiControllers
{
    [ApiController]
    [Route("transaction-template")]
    [Authorize]
    public class TransactionTemplatesController : ControllerBase
    {
        private readonly IMediator _mediator;

        public TransactionTemplatesController(IMediator mediator)
        {
            _mediator = mediator;
        }

        [HttpGet("is-alive")]
        [AllowAnonymous]
        public string IsAlive()
        {
            return DateTime.UtcNow.ToString(CultureInfo.CurrentCulture);
        }

        [HttpGet("get-list")] public async Task<GetTransactionTemplateList.Result> GetTransactionList([FromQuery] GetTransactionTemplateList.Query query) => await _mediator.Send(query);
        [HttpPost("create")] public async Task<CreateTransactionTemplate.Result> CreateTransaction([FromBody] CreateTransactionTemplate.Command command) => await _mediator.Send(command);
        [HttpPost("remove")] public async Task<RemoveTransactionTemplate.Result> RemoveTransaction([FromBody] RemoveTransactionTemplate.Command command) => await _mediator.Send(command);
        [HttpPatch("update/description")] public async Task<UpdateTransactionTemplateDescription.Result> UpdateTransactionDescription([FromBody] UpdateTransactionTemplateDescription.Command command) => await _mediator.Send(command);
        [HttpPatch("update/amount")] public async Task<UpdateTransactionTemplateAmount.Result> UpdateTransactionAmount([FromBody] UpdateTransactionTemplateAmount.Command command) => await _mediator.Send(command);
        [HttpPatch("update/budget-category")] public async Task<UpdateTransactionTemplateCategory.Result> UpdateTransactionCategory([FromBody] UpdateTransactionTemplateCategory.Command command) => await _mediator.Send(command);
    }
}