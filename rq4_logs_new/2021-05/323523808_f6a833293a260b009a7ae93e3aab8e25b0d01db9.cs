using Microsoft.EntityFrameworkCore;
using MyRestaurant.Core;
using System;
using Xunit;

namespace MyRestaurant.SeedData.Tests
{
    public class DatabaseFixture : IDisposable
    {
        private readonly MyRestaurantContext _myRestaurantContext;
        public readonly string ConnectionString;
        private readonly string _sereverName = "localhost";
        private readonly string _userName = "sa";
        private readonly string _password = "1z2x3c!";
        private bool _disposed;

        public DatabaseFixture()
        {
            ConnectionString = $"Server={_sereverName};Database={Guid.NewGuid().ToString()};User={_userName};Password={_password}";

            var builder = new DbContextOptionsBuilder<MyRestaurantContext>();

            builder.UseSqlServer(ConnectionString);
            _myRestaurantContext = new MyRestaurantContext(builder.Options);

            _myRestaurantContext.Database.Migrate();
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // remove the temp db from the server once all tests are done
                    _myRestaurantContext.Database.EnsureDeleted();
                }

                _disposed = true;
            }
        }
    }

    [CollectionDefinition("Database")]
    public class DatabaseCollection : ICollectionFixture<DatabaseFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}