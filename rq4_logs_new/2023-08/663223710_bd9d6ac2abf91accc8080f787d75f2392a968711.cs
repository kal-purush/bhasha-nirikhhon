using Fossa.API.Core.Entities;
using Fossa.API.Core.Messages.Commands;
using Fossa.API.Core.Messages.Queries;
using Fossa.API.Web.ApiModels;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Fossa.API.Web.Api;

[Route("api/[controller]")]
[ApiController]
public class CompanyController : BaseApiController
{
  public CompanyController(
    ISender sender,
    IPublisher publisher) : base(sender, publisher)
  {
  }

  [HttpDelete("{id}")]
  public async Task DeleteAsync(long id, CancellationToken cancellationToken)
  {
    var tenantId = Guid.NewGuid();
    await _sender.Send(
      new CompanyDeletionCommand(id, tenantId),
      cancellationToken);
  }

  [HttpGet]
  public async Task<CompanyEntity> GetAsync(
    CancellationToken cancellationToken)
  {
    var tid = Guid.NewGuid();
    return await _sender.Send(
      new CompanyRetrievalQuery(tid),
      cancellationToken);
  }

  [HttpPost]
  public async Task PostAsync(
    [FromBody] CompanyModel model,
    CancellationToken cancellationToken)
  {
    var tenantId = Guid.NewGuid();
    await _sender.Send(
      new CompanyCreationCommand(tenantId, model.Name),
      cancellationToken);
  }

  [HttpPut("{id}")]
  public async Task PutAsync(
    long id,
    [FromBody] CompanyModel model,
    CancellationToken cancellationToken)
  {
    var tenantId = Guid.NewGuid();
    await _sender.Send(
      new CompanyModificationCommand(id, tenantId, model.Name),
      cancellationToken);
  }
}