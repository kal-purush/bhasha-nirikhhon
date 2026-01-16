using Fossa.API.Core.Entities;
using Fossa.API.Core.Repositories;
using MediatR;

namespace Fossa.API.Core.Messages.Queries;

public class CompanyRetrievalQueryHandler : IRequestHandler<CompanyRetrievalQuery, CompanyEntity>
{
  private readonly ICompanyQueryRepository _companyQueryRepository;

  public CompanyRetrievalQueryHandler(
    ICompanyQueryRepository companyQueryRepository)
  {
    _companyQueryRepository = companyQueryRepository ?? throw new ArgumentNullException(nameof(companyQueryRepository));
  }

  public async Task<CompanyEntity> Handle(
    CompanyRetrievalQuery request,
    CancellationToken cancellationToken)
  {
    return await _companyQueryRepository.GetByTenantIdAsync(
      request.TenantID, cancellationToken)
      .ConfigureAwait(false);
  }
}