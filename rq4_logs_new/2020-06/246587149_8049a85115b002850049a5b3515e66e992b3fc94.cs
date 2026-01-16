using Polly;

namespace Solari.Ganymede.Framework
{
    public interface IGanymedePolicyRegistry
    {
        IGanymedePolicyRegistry AddPolicy(string key, IAsyncPolicy policy);
        IGanymedePolicyRegistry AddPolicy<T>(string key, IAsyncPolicy<T> policy);
        IGanymedePolicyRegistry AddPolicy(string key, ISyncPolicy policy);
        IGanymedePolicyRegistry AddPolicy<T>(string key, ISyncPolicy<T> policy);
        IGanymedePolicyRegistry ClearRegistry();
        IGanymedePolicyRegistry RemovePolicy(string key);
    }
}