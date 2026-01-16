using Microsoft.EntityFrameworkCore;
using PocketForzaHorizonCommunity.Back.Database;
using PocketForzaHorizonCommunity.Back.Database.Entities;
using PocketForzaHorizonCommunity.Back.Database.Repos.Interfaces;
using PocketForzaHorizonCommunity.Back.Services.Extensions;

namespace PocketForzaHorizonCommunity.Back.Services.Services;

public abstract class ServiceBase<TRepo, TEntity> where TEntity : EntityBase where TRepo : IRepositoryBase<TEntity>
{
    protected IRepositoryBase<TEntity> _repository;
    public ServiceBase(IRepositoryBase<TEntity> repository)
    {
        _repository = repository;
    }

    public virtual async Task<PaginationModel<TEntity>> GetAllAsync(int page, int pageSize)
    {
        return await _repository.GetAll().PaginateAsync(page, pageSize);
    }

    public virtual async Task<TEntity> GetByIdAsync(Guid Id)
    {
        var entity = await _repository.GetById(Id).FirstOrDefaultAsync();

        if (entity == null)
        {
            throw new Exception();
        }

        return entity;
    }

    public virtual async Task<TEntity> CreateAsync(TEntity entity)
    {
        await _repository.CreateAsync(entity);
        await _repository.SaveAsync();

        return entity;
    }

    public virtual async Task DeleteAsync(Guid id)
    {
        var entity = await _repository.GetById(id).FirstOrDefaultAsync();

        if (entity == null)
        {
            throw new Exception();
        }

        _repository.Delete(entity);
        await _repository.SaveAsync();
    }

    public abstract Task<TEntity> UpdateAsync(TEntity entity);
}