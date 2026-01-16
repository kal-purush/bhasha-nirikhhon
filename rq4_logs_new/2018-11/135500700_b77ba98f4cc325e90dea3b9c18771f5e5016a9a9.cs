using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BankWithUs.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;

namespace BankWithUs.Data
{
    public interface IBankContext
    {
        IQueryable<Account> Accounts { get; }
        void AddEntity<TEntity>(TEntity entity) where TEntity : class;
        Task<int> SaveChangesAsync(CancellationToken cancellationToken = default(CancellationToken));
        Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default(CancellationToken));
        EntityEntry Update(object entity);
        EntityEntry<TEntity> Update<TEntity>(TEntity entity) where TEntity : class;
        void Remove(Account account);
    }
}