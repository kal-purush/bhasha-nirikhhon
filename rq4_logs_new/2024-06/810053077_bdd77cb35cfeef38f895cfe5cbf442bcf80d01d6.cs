using backend_carhelp.Workshop_management.Domain.Model.Aggregates;
using backend_carhelp.Workshop_management.Domain.Model.Queries;
using backend_carhelp.Workshop_management.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Services;

namespace backend_carhelp.Workshop_management.Application.Internal.QueryServices;

public class AdvertisingQueryService(IAdvertisingRepository advertisingRepository): IAdvertisingQueryService
{
    public async Task<IEnumerable<Advertising>> Handle(GetAllAdvertisingQuery query)
    {
        return await advertisingRepository.ListAsync();
    }
    public async Task<Advertising?> Handle(GetAdvertisingByIdQuery query)
    {
        return await advertisingRepository.FindByIdAsync(query.AdvertisingId);
    }
}