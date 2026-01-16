using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Lykke.Tools.BlockchainBalancesReport.Clients.Nemchina;
using Lykke.Tools.BlockchainBalancesReport.Clients.Steemit.Contracts;
using Lykke.Tools.BlockchainBalancesReport.Configuration;
using Microsoft.Extensions.Options;

namespace Lykke.Tools.BlockchainBalancesReport.Blockchains.Steem
{
    public class SteemBalanceProvider : IBalanceProvider
    {
        private readonly string _baseUrl;
        private readonly Asset _steemAsset;
        private const int Precision = 6;

        // ReSharper disable once UnusedMember.Global
        public SteemBalanceProvider(IOptions<SteemSettings> settings) :
            this(settings.Value.SteemetBaseUrl)
        {
        }

        public SteemBalanceProvider(string baseUrl)
        {
            _baseUrl = baseUrl;
            
            _steemAsset = new Asset("STEEM", "STEEM", "72da9464-49d0-4f95-983d-635c04e39f3c");
        }

        public string BlockchainType => "Steem";

        public  async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            var result = 0m;
            var page = 0;
            var proccedNext = true;

            var history = new List<AccountHistoryResponse>();
            while (proccedNext)
            {
                page++;
                var batch = await _baseUrl.PostJsonAsync
                (
                    new
                    {
                        address,
                        page
                    }
                ).ReceiveJson<AccountHistoryResponse[]>();
                
                history.AddRange(batch);
                
                proccedNext = batch.Any();
            }

            decimal Align(decimal value)
            {
                return value / (decimal) (Math.Pow(10, Precision));
            }

            //foreach (var entry in history.Where(p => DateTimeOffset.FromUnixTimeSeconds(p.TimeStamp) <= at))
            //{

            //    var alignedAmount = Align(entry.Amount);

            //    decimal balanceChange;

            //    var isIncomingAmount = string.Equals(address, entry.Recipient);
            //    if (isIncomingAmount)
            //    {
            //        balanceChange = alignedAmount;
            //    }
            //    else
            //    {
            //        alignedAmount += Align(entry.Fee);
            //        balanceChange = alignedAmount * -1;
            //    }
            //    result += balanceChange;
            //}

            return new Dictionary<Asset, decimal>
            {
                {_steemAsset, result}
            };
        }
    }
}