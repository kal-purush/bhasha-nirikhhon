using Superkatten.Katministratie.Domain.Entities;
using System.ComponentModel;

using ContractEntities = Superkatten.Katministratie.Contract.Entities;

namespace Superkatten.Katministratie.Application.Mappers;

public class CatchOriginMapper : ICatchOriginMapper
{
    public ContractEntities.CatchOrigin MapDomainToContract(CatchOrigin catchOrigin)
    {
        return new ContractEntities.CatchOrigin
        {
            Id = catchOrigin.Id,
            Name = catchOrigin.Name,
            Type = MapDomainToContract(catchOrigin.Type)
        };
    }

    public CatchOrigin MapContractToDomain(ContractEntities.CatchOrigin catchOrigin)
    {
        var catchOriginType = MapContractToDomain(catchOrigin.Type);
        return new CatchOrigin(catchOrigin.Name, catchOriginType)
        {
            Id = catchOrigin.Id
        };
    }

    private static ContractEntities.CatchOriginType MapDomainToContract(CatchOriginType catchOriginType)
    {
        return catchOriginType switch
        {
            CatchOriginType.Farm => ContractEntities.CatchOriginType.Farm,
            CatchOriginType.PrivateProperty => ContractEntities.CatchOriginType.PrivateProperty,
            CatchOriginType.AllotmentGarden => ContractEntities.CatchOriginType.AllotmentGarden,
            CatchOriginType.Camping => ContractEntities.CatchOriginType.Camping,
            CatchOriginType.Farmhouse => ContractEntities.CatchOriginType.Farmhouse,
            CatchOriginType.Stable => ContractEntities.CatchOriginType.Stable,
            CatchOriginType.BusinessPark => ContractEntities.CatchOriginType.BusinessPark,
            CatchOriginType.UrbanArea => ContractEntities.CatchOriginType.UrbanArea,
            CatchOriginType.RuralArea => ContractEntities.CatchOriginType.RuralArea,
            CatchOriginType.NatureReserve => ContractEntities.CatchOriginType.NatureReserve,
            _ => throw new InvalidEnumArgumentException(nameof(catchOriginType), (int)catchOriginType, typeof(CatchOriginType))
        };
    }

    public CatchOriginType MapContractToDomain(ContractEntities.CatchOriginType catchOriginType)
    {
        return catchOriginType switch
        { 
            ContractEntities.CatchOriginType.Farm => CatchOriginType.Farm,
            ContractEntities.CatchOriginType.PrivateProperty => CatchOriginType.PrivateProperty,
            ContractEntities.CatchOriginType.AllotmentGarden => CatchOriginType.AllotmentGarden,
            ContractEntities.CatchOriginType.Camping => CatchOriginType.Camping,
            ContractEntities.CatchOriginType.Farmhouse => CatchOriginType.Farmhouse,
            ContractEntities.CatchOriginType.Stable => CatchOriginType.Stable,
            ContractEntities.CatchOriginType.BusinessPark => CatchOriginType.BusinessPark,
            ContractEntities.CatchOriginType.UrbanArea => CatchOriginType.UrbanArea,
            ContractEntities.CatchOriginType.RuralArea => CatchOriginType.RuralArea,
            ContractEntities.CatchOriginType.NatureReserve => CatchOriginType.NatureReserve,
            _ => throw new InvalidEnumArgumentException(nameof(catchOriginType), (int)catchOriginType, typeof(ContractEntities.CatchOriginType))
        };
    }
}