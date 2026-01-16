using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using WeatherAcquisition.Interfaces.Base.Entities;

namespace WeatherAcquisition.Interfaces.Base.Repositories
{
    public interface IRepository<T> where T : IEntity
    {
        Task<bool> ExistId(int Id);

        Task<bool> Exist(T item);

        Task<int> GetCount();

        Task<IEnumerable<T>> GetAll();

        Task<IEnumerable<T>> Get(int Skip, int Count);

        Task<IPage<T>> GetPage(int PageIndex, int PageSize);

        Task<T> GetById(int Id);

        Task<T> Add(T item);

        Task<T> Update(T item);

        Task<T> Delete(T item);

        Task<T> DeleteById(int Id);
    }

    public interface IPage<out T>
    {
        IEnumerable<T> Items { get; }

        int TotalCount { get; }

        int PageIndex { get; }

        int PageSize { get; }

        int TotalPagesCount { get; }
    }
}