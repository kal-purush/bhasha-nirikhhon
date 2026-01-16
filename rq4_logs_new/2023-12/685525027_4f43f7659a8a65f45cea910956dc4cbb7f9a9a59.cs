using Chirp.Infrastructure.Repositories;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;
using Microsoft.EntityFrameworkCore;

namespace Chirp.Infrastructure.Tests
{
	public class AuthorRepositoryIntegrationTests : IAsyncLifetime
	{
		public required IAuthorRepository _authorRepository;
		public required ChirpDBContext _context;
		public required SqlConnection _connection;

		private readonly MsSqlContainer _msSqlContainer = new MsSqlBuilder().Build();

		// From IAsyncLifetime
		public async Task InitializeAsync()
		{	
			await _msSqlContainer.StartAsync();
			_connection = new SqlConnection(_msSqlContainer.GetConnectionString());
			await _connection.OpenAsync();
			var contextOptions = new DbContextOptionsBuilder<ChirpDBContext>()
				.UseSqlServer(_connection)
				.Options;

			// Create the schema and seed some data
			_context = new ChirpDBContext(contextOptions);

			_context.Database.EnsureCreated();
			DbInitializer.SeedDatabase(_context);

			_authorRepository = new AuthorRepository(_context);
		}

		public async Task DisposeAsync()
		{
			await _msSqlContainer.DisposeAsync();
			await _connection.DisposeAsync();
		}

        [Fact]
        public void GetFollowing_AfterFollowAuthor_ReturnsFollowing()
        {
            // Arrange
            var authorToFollow = _authorRepository.GetAuthorByName("Ramus");
            var authorAsFollowee = _authorRepository.GetAuthorByName("Helge");

            // Act
            //_authorRepository.FollowAuthor(authorToFollow, authorAsFollowee)
        }
    }
}