using Fossa.API.Core.Entities;
using Fossa.API.Core.Repositories;
using MediatR;

namespace Fossa.API.Core.Messages.Commands;

public class CompanyDeletionCommandHandler : IRequestHandler<CompanyDeletionCommand>
{
  private readonly ICompanyRepository _companyRepository;

  public CompanyDeletionCommandHandler(
    ICompanyRepository companyRepository)
  {
    _companyRepository = companyRepository ?? throw new ArgumentNullException(nameof(companyRepository));
  }

  public async Task Handle(
    CompanyDeletionCommand request,
    CancellationToken cancellationToken)
  {
    CompanyEntity entity = new(request.ID, request.TenantID, string.Empty);
    await _companyRepository.RemoveAsync(entity, cancellationToken).ConfigureAwait(false);
  }
}