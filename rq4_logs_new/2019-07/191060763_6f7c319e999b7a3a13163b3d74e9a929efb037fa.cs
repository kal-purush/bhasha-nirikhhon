using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Lykke.Tools.BlockchainBalancesReport.Clients.BitsharesExploler;
using Lykke.Tools.BlockchainBalancesReport.Configuration;
using Microsoft.Extensions.Options;

namespace Lykke.Tools.BlockchainBalancesReport.Blockchains.Bitshares
{
    public class BitsharesBalanceProvider : IBalanceProvider
    {
        private readonly string _baseUrl;
        private readonly Dictionary<string, (Asset asset, int precision)> _cachedAssets;

        private readonly Dictionary<string, (string assetName, string lykkeAssetId)> _predefinedAssets;

        // ReSharper disable once UnusedMember.Global
        public BitsharesBalanceProvider(IOptions<BitsharesSettings> settings) :
            this(settings.Value.ExplolerBaseUrl)
        {
        }

        public BitsharesBalanceProvider(string baseUrl)
        {
            _baseUrl = baseUrl;
            
            _predefinedAssets = new Dictionary<string, (string assetName, string lykkeAssetId)>
            {
                {"1.3.0", (assetName: "BTS", lykkeAssetId:"20ce0468-917e-4097-abba-edf7c8600cfb")}
            };

            _cachedAssets = new Dictionary<string, (Asset asset, int precision)>();
        }

        public string BlockchainType => "Bitshares";

        public  async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            var result = new Dictionary<Asset, decimal>();

            var page = 0;
            var proccedNext = true;

            var history = new List<AccountHistoryResponse>();
            while (proccedNext)
            {
                var batch = await _baseUrl.AppendPathSegment("account_history").SetQueryParams
                (
                    new
                    {
                        account_id = address,
                        page
                    }
                ).GetJsonAsync<AccountHistoryResponse[]>();
                
                history.AddRange(batch);
                page++;





                proccedNext = batch.Any();
            }


            foreach (var entry in history
                .Where(p => p.Timestamp <= at && p.Op.Amount != null)
                .OrderByDescending(p => p.Timestamp))
            {
                var assetInfo = await GetAssetInfoAsync(entry.Op.Amount.AssetId);

                var sum = result.ContainsKey(assetInfo.asset) ? result[assetInfo.asset] : 0m;

                var isIncomingAmount = string.Equals(address, entry.Op.To);


                var alignedAmount = entry.Op.Amount.Value / (decimal) (Math.Pow(10, assetInfo.precision));

                if (assetInfo.asset.BlockchainId == "1.3.0")
                {
                    alignedAmount = Math.Floor(alignedAmount);
                }

                decimal balanceChange;
                if (isIncomingAmount)
                {
                    balanceChange = alignedAmount;
                }
                else
                {
                    balanceChange = alignedAmount * -1;
                }

                sum += balanceChange;
                result[assetInfo.asset] = sum;
            }



            return result;
        }

        private async Task<(Asset asset, int precision)> GetAssetInfoAsync(string assetId)
        {
            if (_cachedAssets.ContainsKey(assetId))
            {
                return _cachedAssets[assetId];
            }

            (Asset asset, int precision) result;
            if (_predefinedAssets.TryGetValue(assetId, out var data))
            {
                result =  (asset: new Asset(data.assetName, assetId, data.lykkeAssetId), precision: 5);
            }
            else
            {
                var resp = await _baseUrl.AppendPathSegment("asset").SetQueryParams
                (
                    new
                    {
                        asset_id = assetId
                    }
                ).GetJsonAsync<AssetResponse>();

                result = (new Asset(resp.Symbol, assetId, null), resp.Precision);
            }

            _cachedAssets[assetId] = result;
            return result;
        }
    }
}