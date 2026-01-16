using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.PayTransactionHandler.Core.Domain.TransactionStateCache;
using Lykke.Job.PayTransactionHandler.Core.Extensions;
using Lykke.Job.PayTransactionHandler.Core.Services;
using Lykke.Service.PayInternal.Client;
using Lykke.Service.PayInternal.Client.Models.Wallets;
using Microsoft.Extensions.Caching.Memory;

namespace Lykke.Job.PayTransactionHandler.Services.Transactions
{
    public class TransactionStateCacheMaintainer : ICacheMaintainer<TransactionState>
    {
        //todo: replace with IDistributedCache 
        private readonly IMemoryCache _cache;
        private readonly IPayInternalClient _payInternalClient;
        private readonly ILog _log;
        private readonly int _confirmationsLimit;

        private const string CachePartitionName = "TransactionsState";

        public TransactionStateCacheMaintainer(
            IMemoryCache cache,
            IPayInternalClient payInternalClient,
            int confirmationsLimit,
            ILog log)
        {
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _payInternalClient = payInternalClient ?? throw new ArgumentNullException(nameof(payInternalClient));
            _confirmationsLimit = confirmationsLimit;
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public async Task WarmUp()
        {
            IEnumerable<TransactionStateResponse> transactionsState =
                await _payInternalClient.GetAllMonitoredTransactions();

            foreach (var txStateResponse in transactionsState)
            {
                TransactionState txState = txStateResponse.ToDomainState();

                await _cache.SetWithPartitionAsync(CachePartitionName, txState.Transaction.Id, txState);
            }
        }

        public async Task Wipe()
        {
            IEnumerable<TransactionState> cached = await _cache.GetPartitionAsync<TransactionState>(CachePartitionName);

            IEnumerable<TransactionState> toRemove =
                cached.Where(x => x.IsConfirmed(_confirmationsLimit) || x.IsExpired());

            foreach (TransactionState txState in toRemove)
            {
                if (txState.IsExpired())
                {
                    //todo: notify PayInternal about transaction expired by DueDate
                }

                await _cache.RemoveWithPartitionAsync(CachePartitionName, txState.Transaction.Id);

                await _log.WriteInfoAsync(nameof(TransactionStateCacheMaintainer), nameof(Wipe),
                    $"Cleared transactions {txState.Transaction.Id} from cache with dudate = {txState.DueDate}");
            }
        }

        public async Task UpdateItem(TransactionState item)
        {
            await _cache.SetWithPartitionAsync(CachePartitionName, item.Transaction.Id, item);

            await _log.WriteInfoAsync(nameof(TransactionStateCacheMaintainer), nameof(UpdateItem),
                $"Updated transaction {item.Transaction.Id} in cache");
        }

        public async Task<IEnumerable<TransactionState>> Get()
        {
            return await _cache.GetPartitionAsync<TransactionState>(CachePartitionName);
        }
    }
}