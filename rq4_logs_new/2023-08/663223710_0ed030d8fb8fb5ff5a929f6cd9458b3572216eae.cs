using Fossa.API.Core.Entities;
using Fossa.API.Core.Repositories;
using MediatR;

namespace Fossa.API.Core.Messages.Commands;

public class CompanyModificationCommandHandler : IRequestHandler<CompanyModificationCommand>
{
  private readonly ICompanyRepository _companyRepository;

  public CompanyModificationCommandHandler(
    ICompanyRepository companyRepository)
  {
    _companyRepository = companyRepository ?? throw new ArgumentNullException(nameof(companyRepository));
  }

  public async Task Handle(
    CompanyModificationCommand request,
    CancellationToken cancellationToken)
  {
    CompanyEntity entity = new(request.ID, request.TenantID, request.Name);
    await _companyRepository.UpdateAsync(entity, cancellationToken).ConfigureAwait(false);
  }
}