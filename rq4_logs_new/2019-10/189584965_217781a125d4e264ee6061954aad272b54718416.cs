using System;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Lykke.Common.MsSql
{
    /// <summary>
    /// Fake sql context factory implementation for tests.
    /// </summary>
    public class SqlContextFactoryFake<T> : IDbContextFactory<T>, ITransactionRunner
        where T : DbContext
    {
        private readonly DbContextOptions<T> _dbContextOptions;
        private readonly Func<DbContextOptions, T> _dbContextResolver;

        /// <summary>C-tor</summary>
        public SqlContextFactoryFake(Func<DbContextOptions, T> dbContextResolver, string inMemoryDbName = "1")
        {
            _dbContextResolver = dbContextResolver ?? throw new ArgumentNullException(nameof(dbContextResolver));
            _dbContextOptions = new DbContextOptionsBuilder<T>()
                .UseInMemoryDatabase(inMemoryDbName)
                .Options;
        }

        /// <inheritdoc cref="IDbContextFactory{T}"/>
        public T CreateDataContext()
        {
            return _dbContextResolver(_dbContextOptions);
        }

        /// <inheritdoc cref="IDbContextFactory{T}"/>
        public T CreateDataContext(TransactionContext transactionContext)
        {
            return _dbContextResolver(_dbContextOptions);
        }

        /// <inheritdoc cref="ITransactionRunner"/>
        public Task<TK> RunWithTransactionAsync<TK>(Func<TransactionContext, Task<TK>> func)
        {
            return func(null);
        }

        /// <inheritdoc cref="ITransactionRunner"/>
        public Task RunWithTransactionAsync(Func<TransactionContext, Task> action)
        {
            return action(null);
        }
    }
}