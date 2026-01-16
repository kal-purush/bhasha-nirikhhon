using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper.Wrappers.DependencyInjection;
using Dapper.Wrappers.Formatters;
using Dapper.Wrappers.Generators;
using Dapper.Wrappers.Tests.DbModels;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Npgsql;
using Xunit;

namespace Dapper.Wrappers.Tests.Generators
{
    public class UpdateQueryGeneratorTests : IClassFixture<DatabaseFixture>, IDisposable
    {
        private readonly DatabaseFixture _databaseFixture;
        private readonly IDbConnection _sqlConnection;
        private readonly IDbConnection _postgresConnection;

        public UpdateQueryGeneratorTests(DatabaseFixture databaseFixture, IEnumerable<IDbConnection> connections)
        {
            _databaseFixture = databaseFixture;
            var dbConnections = connections.ToList();
            _sqlConnection = dbConnections.FirstOrDefault(c => c is SqlConnection);
            _postgresConnection = dbConnections.FirstOrDefault(c => c is NpgsqlConnection);
        }

        private TestUpdateQueryGenerator GetTestInstance(SupportedDatabases dbType, string updateQueryString,
            IDictionary<string, MergeOperationMetadata> updateMetadata,
            IDictionary<string, QueryOperationMetadata> filterMetadata)
        {
            var formatter = _databaseFixture.GetFormatter(dbType);

            return new TestUpdateQueryGenerator(formatter, updateQueryString, updateMetadata, filterMetadata);
        }

        private IDbConnection GetConnection(SupportedDatabases dbType) =>
            dbType == SupportedDatabases.SqlServer ? _sqlConnection : _postgresConnection;

        private IQueryContext GetQueryContext(SupportedDatabases dbType)
        {
            var connection = GetConnection(dbType);

            return new QueryContext(connection);
        }

        [Theory]
        [InlineData(SupportedDatabases.SqlServer, "Mistborn: The Final Empire", 647, "Brandon Sanderson")]
        [InlineData(SupportedDatabases.SqlServer, "Arcanum Unbounded", 672, "Brandon Sanderson")]
        [InlineData(SupportedDatabases.SqlServer, "The Sign of the Four", 122, "Arthur Conan Doyle")]
        [InlineData(SupportedDatabases.SqlServer, "The Memoirs of Sherlock Holmes", 208, "Arthur Conan Doyle")]
        [InlineData(SupportedDatabases.SqlServer, "One Fish Two Fish Red Fish Blue Fish", 62, "Dr. Seuss")]
        [InlineData(SupportedDatabases.SqlServer, "Green Eggs and Ham", 62, "Dr. Seuss")]
        [InlineData(SupportedDatabases.PostgreSQL, "Mistborn: The Final Empire", 647, "Brandon Sanderson")]
        [InlineData(SupportedDatabases.PostgreSQL, "Arcanum Unbounded", 672, "Brandon Sanderson")]
        [InlineData(SupportedDatabases.PostgreSQL, "The Sign of the Four", 122, "Arthur Conan Doyle")]
        [InlineData(SupportedDatabases.PostgreSQL, "The Memoirs of Sherlock Holmes", 208, "Arthur Conan Doyle")]
        [InlineData(SupportedDatabases.PostgreSQL, "One Fish Two Fish Red Fish Blue Fish", 62, "Dr. Seuss")]
        [InlineData(SupportedDatabases.PostgreSQL, "Green Eggs and Ham", 62, "Dr. Seuss")]
        public async Task AddUpdateQuery_WithoutFilters_UpdatesAllRows(SupportedDatabases dbType, string nameValue,
            int? pageCountValue, string authorName)
        {
            // Arrange
            var testId = Guid.NewGuid();
            var connection = GetConnection(dbType);

            var testData = GeneratorTestConstants.TestData.GetTestData();

            var books = testData.Books.ToList();
            var authors = testData.Authors.ToList();

            await _databaseFixture.AddGenres(connection, dbType, testId, testData.Genres);
            await _databaseFixture.AddAuthors(connection, dbType, testId, authors);
            await _databaseFixture.AddBooks(connection, dbType, testId, books);
            await _databaseFixture.AddBookGenres(connection, dbType, testId, testData.BookGenres);

            var query = dbType == SupportedDatabases.SqlServer
                ? SqlQueryFormatConstants.SqlServer.Books.UpdateQuery
                : SqlQueryFormatConstants.Postgres.Books.UpdateQuery;

            var operationMetadata = dbType == SupportedDatabases.SqlServer
                ? GeneratorTestConstants.SqlServer.DefaultBookUpdateMetadata
                : GeneratorTestConstants.Postgres.DefaultBookUpdateMetadata;

            var filterMetadata = dbType == SupportedDatabases.SqlServer
                ? GeneratorTestConstants.SqlServer.DefaultBookFilterMetadata
                : GeneratorTestConstants.Postgres.DefaultBookFilterMetadata;

            var generator = GetTestInstance(dbType, query, operationMetadata, filterMetadata);
            var context = GetQueryContext(dbType);

            var authorIdValue = authors.First(a => $"{a.FirstName} {a.LastName}" == authorName).AuthorID;

            // Act
            var operations = new[]
            {
                new QueryOperation
                {
                    Name = "Name",
                    Parameters = new Dictionary<string, object>
                    {
                        {"Name", nameValue}
                    }
                },
                new QueryOperation
                {
                    Name = "AuthorID",
                    Parameters = new Dictionary<string, object>
                    {
                        {"AuthorID", authorIdValue}
                    }
                },
                new QueryOperation
                {
                    Name = "PageCount",
                    Parameters = new Dictionary<string, object>
                    {
                        {"PageCount", pageCountValue}
                    }
                }
            };

            var filterOperations = new QueryOperation[]
            {
                new QueryOperation
                {
                    Name = "TestIDEquals",
                    Parameters = new Dictionary<string, object>
                    {
                        {"TestID", testId}
                    }
                }
            };

            generator.AddUpdateQuery(context, operations, filterOperations);

            await context.ExecuteCommands();

            // Assert
            var updatedBooks = (await _databaseFixture.GetBooks(connection, dbType, testId)).ToList();

            updatedBooks.Should().NotContain(book =>
                book.Name != nameValue || book.AuthorID != authorIdValue || book.PageCount != pageCountValue);
        }

        public void Dispose()
        {
            _sqlConnection?.Dispose();
            _postgresConnection?.Dispose();
        }
    }

    public class TestUpdateQueryGenerator : UpdateQueryGenerator
    {
        public TestUpdateQueryGenerator(IQueryFormatter queryFormatter, string updateQueryString,
            IDictionary<string, MergeOperationMetadata> updateMetadata,
            IDictionary<string, QueryOperationMetadata> filterMetadata) : base(queryFormatter)
        {
            UpdateQueryString = updateQueryString;
            UpdateOperationMetadata = updateMetadata;
            FilterOperationMetadata = filterMetadata;
        }

        protected override IDictionary<string, QueryOperationMetadata> FilterOperationMetadata { get; }
        protected override string UpdateQueryString { get; }
        protected override IDictionary<string, MergeOperationMetadata> UpdateOperationMetadata { get; }
    }
}