using Fossa.API.Core.Entities;
using Fossa.API.Core.Identity;
using Fossa.API.Core.Repositories;
using MediatR;

namespace Fossa.API.Core.Messages.Commands;

public class CompanyCreationCommandHandler : IRequestHandler<CompanyCreationCommand>
{
  private readonly ICompanyRepository _companyRepository;
  private readonly IIdentityGenerator<long> _identityGenerator;

  public CompanyCreationCommandHandler(
    IIdentityGenerator<long> identityGenerator,
    ICompanyRepository companyRepository)
  {
    _identityGenerator = identityGenerator ?? throw new ArgumentNullException(nameof(identityGenerator));
    _companyRepository = companyRepository ?? throw new ArgumentNullException(nameof(companyRepository));
  }

  public async Task Handle(
    CompanyCreationCommand request,
    CancellationToken cancellationToken)
  {
    var id = _identityGenerator.Generate();
    CompanyEntity entity = new(id, request.TenantID, request.Name);
    await _companyRepository.AddAsync(entity, cancellationToken).ConfigureAwait(false);
  }
}