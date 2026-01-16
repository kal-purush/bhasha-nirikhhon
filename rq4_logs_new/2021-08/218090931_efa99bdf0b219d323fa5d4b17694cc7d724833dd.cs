using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper.Wrappers.DependencyInjection;
using Dapper.Wrappers.Formatters;
using Dapper.Wrappers.Generators;
using Dapper.Wrappers.Tests.DbModels;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace Dapper.Wrappers.Tests.Generators
{
    public class GetQueryGeneratorTests
    {
        private readonly DatabaseFixture _databaseFixture;
        private readonly IDbConnection _sqlConnection;
        private readonly IDbConnection _postgresConnection;

        public GetQueryGeneratorTests(DatabaseFixture databaseFixture, IEnumerable<IDbConnection> connections)
        {
            _databaseFixture = databaseFixture;
            var dbConnections = connections.ToList();
            _sqlConnection = dbConnections.FirstOrDefault(c => c is SqlConnection);
            _postgresConnection = dbConnections.FirstOrDefault(c => c is NpgsqlConnection);
        }

        private TestGetQueryGenerator GetTestInstance(SupportedDatabases dbType, string queryString,
            string defaultOrdering, IDictionary<string, QueryOperationMetadata> filterMetadata,
            IDictionary<string, QueryOperationMetadata> orderMetadata)
        {
            var formatter = _databaseFixture.GetFormatter(dbType);

            return new TestGetQueryGenerator(formatter, queryString, defaultOrdering, filterMetadata, orderMetadata);
        }

        private IDbConnection GetConnection(SupportedDatabases dbType) =>
            dbType == SupportedDatabases.SqlServer ? _sqlConnection : _postgresConnection;

        private IQueryContext GetQueryContext(SupportedDatabases dbType)
        {
            var connection = GetConnection(dbType);

            return new QueryContext(connection);
        }

        public async Task AddGetQuery_WithoutFiltersOrderingOrPagination_ShouldReturnExpectedResults(
            SupportedDatabases dbType)
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
                ? SqlQueryFormatConstants.SqlServer.Books.Selec
                : SqlQueryFormatConstants.Postgres.Books.UpdateQuery;

            var operationMetadata = dbType == SupportedDatabases.SqlServer
                ? GeneratorTestConstants.SqlServer.DefaultBookUpdateMetadata
                : GeneratorTestConstants.Postgres.DefaultBookUpdateMetadata;

            var filterMetadata = dbType == SupportedDatabases.SqlServer
                ? GeneratorTestConstants.SqlServer.DefaultBookFilterMetadata
                : GeneratorTestConstants.Postgres.DefaultBookFilterMetadata;

            var generator = GetTestInstance(dbType, query, operationMetadata, filterMetadata);
            var context = GetQueryContext(dbType);
        }
    }

    public class TestGetQueryGenerator : GetQueryGenerator
    {
        public TestGetQueryGenerator(IQueryFormatter queryFormatter, string queryString, string defaultOrdering,
            IDictionary<string, QueryOperationMetadata> filterMetadata,
            IDictionary<string, QueryOperationMetadata> orderMetadata) : base(queryFormatter)
        {
            GetQueryString = queryString;
            DefaultOrdering = defaultOrdering;
            FilterOperationMetadata = filterMetadata;
            OrderOperationMetadata = orderMetadata;
        }

        protected override IDictionary<string, QueryOperationMetadata> FilterOperationMetadata { get; }
        protected override string GetQueryString { get; }
        protected override string DefaultOrdering { get; }
        protected override IDictionary<string, QueryOperationMetadata> OrderOperationMetadata { get; }
    }
}