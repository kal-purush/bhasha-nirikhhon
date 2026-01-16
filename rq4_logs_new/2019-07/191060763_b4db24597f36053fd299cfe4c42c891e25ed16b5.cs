using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Tools.BlockchainBalancesReport.Clients.InsightApi;
using Lykke.Tools.BlockchainBalancesReport.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lykke.Tools.BlockchainBalancesReport.Blockchains.Decred
{
    public class DecredBalanceProvider : IBalanceProvider
    {
        public string BlockchainType => "Decred";

        private readonly InsightApiBalanceProvider _balanceProvider;

        public DecredBalanceProvider(
            ILoggerFactory loggerFactory,
            IOptions<LiteCoinSettings> settings)
        {
            _balanceProvider = new InsightApiBalanceProvider
            (
                loggerFactory.CreateLogger<InsightApiBalanceProvider>(),
                new InsightApiClient(settings.Value.InsightApiUrl),
                NormalizeOrDefault
            );
        }

        public async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address,
            DateTime at)
        {
            var balance = await _balanceProvider.GetBalanceAsync(address, at);

            return new Dictionary<Asset, decimal>
            {
                {new Asset("DCR", "DCR", "02154b48-7ed9-4211-b614-e87679fd4f5a"), balance}
            };
        }

        private string NormalizeOrDefault(string address)
        {
            return address;
        }
    }
}