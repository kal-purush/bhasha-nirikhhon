using System.Collections.Generic;
using System.Threading.Tasks;
using Shared.DTO;

namespace BusinessLayer.Interfaces
{
    public interface IServiceCrew
    {
        Task<List<Crew>> Get10Async();

        Task<bool> ValidationForeignIdAsync(Crew ob);

        Task<Crew> IsExistAsync(int id);

        Task<List<Crew>> GetAllAsync();
        
        Task<Crew> GetDetailsAsync(int id);

        Task AddAsync(Crew entity);

        Task UpdateAsync(Crew entity);

        Task RemoveAsync(int id);

        Task RemoveAllAsync();
    }
}