using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Lykke.Tools.BlockchainBalancesReport.Clients.Nemchina;
using Lykke.Tools.BlockchainBalancesReport.Configuration;
using Microsoft.Extensions.Options;

namespace Lykke.Tools.BlockchainBalancesReport.Blockchains.Nem
{
    public class NemBalanceProvider : IBalanceProvider
    {
        private readonly string _baseUrl;
        private readonly Asset _nemAsset;
        private const int Precision = 6;

        // ReSharper disable once UnusedMember.Global
        public NemBalanceProvider(IOptions<NemSettings> settings) :
            this(settings.Value.NemchinaBaseUrl)
        {
        }

        public NemBalanceProvider(string baseUrl)
        {
            _baseUrl = baseUrl;
            
            _nemAsset = new Asset("XEM", "XEM", "903eafbd-cc29-4d60-8d7d-907695d9caae");
        }

        public string BlockchainType => "Nem";

        public  async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            var result = 0m;
            var page = 0;
            var proccedNext = true;

            var history = new List<TransactionsResponse>();
            while (proccedNext)
            {
                page++;
                var batch = await _baseUrl.AppendPathSegment("/account/detailTXList").PostJsonAsync
                (
                    new
                    {
                        address,
                        page
                    }
                ).ReceiveJson<TransactionsResponse[]>();
                
                history.AddRange(batch);
                
                proccedNext = batch.Any();
            }

            decimal Align(decimal value)
            {
                return value / (decimal) (Math.Pow(10, Precision));
            }

            foreach (var entry in history.Where(p => DateTimeOffset.FromUnixTimeSeconds(p.TimeStamp) <= at))
            {

                var alignedAmount = Align(entry.Amount);

                decimal balanceChange;

                var isIncomingAmount = string.Equals(address, entry.Recipient);
                if (isIncomingAmount)
                {
                    balanceChange = alignedAmount;
                }
                else
                {
                    alignedAmount += Align(entry.Fee);
                    balanceChange = alignedAmount * -1;
                }
                result += balanceChange;
            }

            return new Dictionary<Asset, decimal>
            {
                {_nemAsset, result}
            };
        }
    }
}