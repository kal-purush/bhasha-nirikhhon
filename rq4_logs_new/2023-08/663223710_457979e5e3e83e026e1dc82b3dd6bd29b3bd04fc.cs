using Fossa.API.Core.Entities;
using Fossa.API.Persistence.Mongo.Entities;
using TIKSN.Mapping;

namespace Fossa.API.Persistence.Mongo.Mappers;

public class CompanyMongoMapper
  : IMapper<CompanyMongoEntity, CompanyEntity>
  , IMapper<CompanyEntity, CompanyMongoEntity>
{
  public CompanyEntity Map(CompanyMongoEntity source)
  {
    return new(source.ID, source.TenantID, source?.Name ?? string.Empty);
  }

  public CompanyMongoEntity Map(CompanyEntity source)
  {
    return new CompanyMongoEntity
    {
      ID = source.ID,
      TenantID = source.TenantID,
      Name = source.Name,
    };
  }
}