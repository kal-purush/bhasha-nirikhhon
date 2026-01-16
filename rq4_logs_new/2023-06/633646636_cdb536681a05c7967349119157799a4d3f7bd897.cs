using Xunit;

namespace Soenneker.Redis.Lock.Tests;

/// <summary>
/// This class has no code, and is never created. Its purpose is simply
/// to be the place to apply [CollectionDefinition] and all the
/// ICollectionFixture interfaces.
/// </summary>
[CollectionDefinition("RedisLockUtilCollection")]
public class RedisLockUtilCollection : ICollectionFixture<RedisLockUtilFixture>
{
}