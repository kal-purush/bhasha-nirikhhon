namespace InjectableAWS.Repository;

using Amazon.DynamoDBv2.DataModel;
using InjectableAWS;

public abstract class DynamoDbRepository<T> where T : DynamoDbRepositoryOptions {
	private readonly DynamoDbContext<T> _context;
	private readonly DynamoDBOperationConfig _config;

	protected DynamoDbRepository(
		T options,
		DynamoDbContext<T> context
	) {
		if( options is null ) {
			throw new ArgumentNullException( nameof( options ) );
		}

		_context = context;
		_config = new DynamoDBOperationConfig {
			OverrideTableName = options.TableName
		};
	}

	public async Task CreateAsync<TData>(
		TData data,
		CancellationToken cancellationToken
	) {
		await _context.Context
			.SaveAsync(
				data,
				_config,
				cancellationToken
			).ConfigureAwait( false );
	}

	public async Task<TData> LoadAsync<TData>(
		string pk,
		string sk,
		CancellationToken cancellationToken
	) {
		return await _context.Context
			.LoadAsync<TData>(
				pk,
				sk,
				_config,
				cancellationToken
			).ConfigureAwait( false );
	}
}