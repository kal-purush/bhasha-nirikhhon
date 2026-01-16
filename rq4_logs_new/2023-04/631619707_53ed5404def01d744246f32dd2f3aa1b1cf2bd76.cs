using Microsoft.AspNetCore.Http;
using PocketForzaHorizonCommunity.Back.Database;
using PocketForzaHorizonCommunity.Back.Database.Entities;

namespace PocketForzaHorizonCommunity.Back.Services.Services.Interfaces
{
    public interface ServiceWithFilesBase<TEntity> where TEntity : EntityBase
    {
        Task<TEntity> CreateAsync(TEntity entity, IFormFile thumbnail);
        Task Delete(Guid id);
        Task<PaginationModel<TEntity>> GetAllAsync(int page, int pageSize);
        Task<TEntity> GetById(Guid id);
    }
}