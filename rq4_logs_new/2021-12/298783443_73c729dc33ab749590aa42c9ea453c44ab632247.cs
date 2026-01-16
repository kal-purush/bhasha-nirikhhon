using System;
using System.Threading;
using System.Threading.Tasks;
using BobApi;

namespace OldPartitionsRemover.ByDateRemoving.Entities
{
    internal readonly struct NodeApi
    {
        private readonly BobApiClient _bobApiClient;
        private readonly CancellationToken _cancellationToken;

        public NodeApi(BobApiClient bobApiClient, CancellationToken cancellationToken)
        {
            _bobApiClient = bobApiClient;
            _cancellationToken = cancellationToken;
        }

        public async Task<T> Invoke<T>(Func<BobApiClient, CancellationToken, Task<T>> f) => await f(_bobApiClient, _cancellationToken);
    }
}