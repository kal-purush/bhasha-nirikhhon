using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Lykke.Job.BlockchainBalancesReport.Clients.ChainId;
using Lykke.Job.BlockchainBalancesReport.Settings;

namespace Lykke.Job.BlockchainBalancesReport.Blockchains.SolarCoin
{
    public class SolarCoinBalanceProvider : IBalanceProvider
    {
        private readonly string _baseUrl;
        private readonly Asset _nemAsset;

        // ReSharper disable once UnusedMember.Global
        public SolarCoinBalanceProvider(SolarCoinSetting settings) :
            this(settings.ChainzBaseUrl)
        {
        }

        public SolarCoinBalanceProvider(string baseUrl)
        {
            _baseUrl = baseUrl;
            
            _nemAsset = new Asset("SLR", "SLR", "SLR");
        }

        public string BlockchainType => "SLR";

        public  async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            var indexResp = await (_baseUrl.AppendPathSegment("slr/address.dws") + $"?{address}.htm")
                                .GetStringAsync();

            var id = ChainIdDeserializer.GetChainid(indexResp);

            //TODO add rate limiter retry processing 
            var txsResp = await _baseUrl.AppendPathSegment("explorer/address.summary.dws").SetQueryParams
                (
                    new
                    {
                        coin = "slr",
                        id = id
                    }
                )
                .GetStringAsync();

            var history = ChainIdDeserializer.DeserializeTransactionsResp(txsResp);
            var result = 0m;

            foreach (var entry in history.Where(p => p.date <= at))
            {
                result += entry.amount;
            }

            return new Dictionary<Asset, decimal>
            {
                {_nemAsset, result}
            };
        }
    }
}