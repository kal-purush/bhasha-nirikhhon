using ShortSharing.BLL.Abstractions;
using ShortSharing.BLL.Models;
using ShortSharing.DAL.Abstractions;

namespace ShortSharing.BLL.Services;

public class ThingsService : IThingsService
{
    private readonly IGenericRepository<ThingModel> _repository;

    public ThingsService(IGenericRepository<ThingModel> repository)
    {
        _repository = repository;
    }

    public async Task<ThingModel> CreateAsync(ThingModel thingModel)
    {
        return await _repository.CreateAsync(thingModel);
    }

    public async Task DeleteAsync(Guid id)
    {
        await _repository.DeleteAsync(id);
    }

    public async Task<List<ThingModel>> GetAllAsync()
    {
        return await _repository.GetAllAsync();
    }

    public async Task<ThingModel?> GetByIdAsync(Guid id)
    {
        return await _repository.GetByIdAsync(id);
    }

    public async Task<ThingModel?> UpdateAsync(Guid id, ThingModel thingModel)
    {
        return await _repository.UpdateAsync(id, thingModel);
    }
}