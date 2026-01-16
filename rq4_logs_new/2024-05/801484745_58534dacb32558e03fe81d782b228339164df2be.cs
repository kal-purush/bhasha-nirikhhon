using OnionAPI.Application.Interfaces.Repositories;
using OnionAPI.Domain.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnionAPI.Application.Interfaces.IUnitOfWorks
{
    public interface IUnitOfWork : IAsyncDisposable
    {
        IReadRepository<T>GetReadRepository<T>() where T : class, IBaseEntity, new();
        IWriteRepository<T> GetWriteRepository<T>() where T: class, IBaseEntity, new();

        Task<int> SaveAsync();
        int Save();


    }
}