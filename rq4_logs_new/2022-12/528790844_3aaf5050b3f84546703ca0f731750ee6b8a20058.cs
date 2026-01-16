using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections.CosmosDb;

public class CosmosDbProjectionRepositoryFactory : ProjectionRepositoryFactory
{
    private readonly LoggerFactory _loggerFactory;
    private readonly string _connectionString;
    private readonly CosmosClientOptions _cosmosClientOptions;
    private readonly string _databaseId;
    private readonly string _containerId;

    public CosmosDbProjectionRepositoryFactory(
        LoggerFactory loggerFactory,
        string connectionString,
        CosmosClientOptions cosmosClientOptions,
        string databaseId,
        string containerId
    )
    {
        _loggerFactory = loggerFactory;
        _connectionString = connectionString;
        _cosmosClientOptions = cosmosClientOptions;
        _databaseId = databaseId;
        _containerId = containerId;
    }

    public override IProjectionRepository<TProjectionDocument> GetProjectionRepository<TProjectionDocument>()
    {
        return new CosmosDbProjectionRepository<TProjectionDocument>(
            _loggerFactory,
            _connectionString,
            _cosmosClientOptions,
            _databaseId,
            _containerId
        );
    }

    public override IProjectionRepository GetProjectionRepository(ProjectionDocumentSchema projectionDocumentSchema)
    {
        return new CosmosDbProjectionRepository(
            _loggerFactory,
            _connectionString,
            _cosmosClientOptions,
            _databaseId,
            _containerId,
            projectionDocumentSchema
        );
    }
}