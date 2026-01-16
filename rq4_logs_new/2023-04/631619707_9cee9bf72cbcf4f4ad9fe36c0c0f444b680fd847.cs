using Microsoft.EntityFrameworkCore;
using PocketForzaHorizonCommunity.Back.Database;
using PocketForzaHorizonCommunity.Back.Database.Entities.CarEntities;
using PocketForzaHorizonCommunity.Back.Database.Repos.Interfaces;
using PocketForzaHorizonCommunity.Back.Services.Extensions;

namespace PocketForzaHorizonCommunity.Back.Services.Services;

public class CarService : ServiceBase<ICarsRepository, Car>
{
    public CarService(ICarsRepository repository) : base(repository)
    {
    }

    public async Task<PaginationModel<Car>> GetDesignsByIdAsync(Guid id, int page, int pageSize)
    {
        var result = await _repository.GetByIdWithTunes(id).PaginateAsync(page, pageSize);

        if (result == null)
        {
            throw new EntityNotFoundException();
        }

        return result;
    }

    public async Task<PaginationModel<Car>> GetTunesByIdAsync(Guid Id, int page, int pageSize)
    {
        var result = await _repository.GetByIdWithDesigns(Id).PaginateAsync(page, pageSize);

        if (result == null)
        {
            throw new EntityNotFoundException();
        }

        return result;
    }

    public async Task<Car> UpdateAsync(Car newEntity)
    {
        var oldEntity = await _repository.GetById(newEntity.Id).FirstOrDefaultAsync();

        if (oldEntity == null)
        {
            throw new EntityNotFoundException();
        }

        oldEntity.Model = newEntity.Model;
        oldEntity.Year = newEntity.Year;
        oldEntity.Price = newEntity.Price;
        oldEntity.ManufactureId = newEntity.ManufactureId;
        oldEntity.CarTypeId = newEntity.CarTypeId;
        await _repository.SaveAsync();

        return oldEntity;
    }
}