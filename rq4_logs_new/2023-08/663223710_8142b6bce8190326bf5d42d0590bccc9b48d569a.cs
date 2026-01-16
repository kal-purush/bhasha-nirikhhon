using Fossa.API.Core.Entities;
using Fossa.API.Core.Repositories;
using Fossa.API.Persistence.Mongo.Entities;
using TIKSN.Data.Mongo;
using TIKSN.Mapping;

namespace Fossa.API.Persistence.Mongo.Repositories;

public class CompanyRepositoryAdapter
       : MongoRepositoryAdapter<CompanyEntity, long, CompanyMongoEntity, long>
       , ICompanyRepository, ICompanyQueryRepository
{
  public CompanyRepositoryAdapter(
    IMapper<CompanyEntity, CompanyMongoEntity> domainEntityToDataEntityMapper,
    IMapper<CompanyMongoEntity, CompanyEntity> dataEntityToDomainEntityMapper,
    ICompanyMongoRepository mongoRepository) : base(
      domainEntityToDataEntityMapper,
      dataEntityToDomainEntityMapper,
      IdentityMapper<long>.Instance,
      IdentityMapper<long>.Instance,
      mongoRepository)
  {
  }
}