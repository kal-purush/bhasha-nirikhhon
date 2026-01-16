using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Tools.BlockchainBalancesReport.Clients.InsightApi;
using Lykke.Tools.BlockchainBalancesReport.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBitcoin;

namespace Lykke.Tools.BlockchainBalancesReport.Blockchains.Dash
{
    public class DashInsightApiBalanceProvider : IBalanceProvider
    {
        public string BlockchainType => "Dash";

        private readonly Network _network;
        private readonly InsightApiBalanceProvider _balanceProvider;

        public DashInsightApiBalanceProvider(
            ILoggerFactory loggerFactory, 
            IOptions<DashSettings> settings)
        {
            NBitcoin.Altcoins.Dash.Instance.EnsureRegistered();

            _network = NBitcoin.Altcoins.Dash.Instance.Mainnet;
            _balanceProvider = new InsightApiBalanceProvider
            (
                loggerFactory.CreateLogger<InsightApiBalanceProvider>(),
                new InsightApiClient(settings.Value.InsightApiUrl),
                NormalizeOrDefault
            );
        }

        public async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            var balance = await _balanceProvider.GetBalanceAsync(address, at);

            return new Dictionary<Asset, decimal>
            {
                {new Asset("DASH", "DASH", "4d498e43-956f-45ee-be07-8bb435003f26"), balance}
            };
        }

        private string NormalizeOrDefault(string address)
        {
            try
            {
                var bitcoinAddress = BitcoinAddress.Create(address, _network);

                return bitcoinAddress.ToString();
            }
            catch (FormatException)
            {
                return null;
            }
        }
    }
}