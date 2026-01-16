using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace Lykke.Tools.BlockchainBalancesReport.Clients.Steemit
{
    public static class SteemetDeserializer
    {
        public static IEnumerable<(string txId, DateTime timestamp, string from, string to, decimal amount)> DeserializeTransactionsResp(string source)
        {
            var model = JsonConvert.DeserializeObject<TransactionsResp>(source);
            var txs = model.Result.Select(p => p[1]).ToList();

            var ids = new HashSet<string>();

            foreach (var tx in txs)
            {
                var t = JsonConvert.SerializeObject(tx.op);
                var t2 = JsonConvert.DeserializeObject<dynamic[]>(t);
                var op = t2[1];
                if (t2[0] == "transfer")
                {
                    var id = tx.trx_id.ToString();

                    if (!ids.Contains(id))
                    {
                        yield return (
                            id,
                            DateTime.Parse(tx.timestamp.ToString()),
                            op.from.ToString(),
                            op.to.ToString(),
                            ParseSteemValue(op.amount.ToString()));

                        ids.Add(id);
                    }
                }
            }
        }

        private static decimal ParseSteemValue(string sourceValue)
        {
            return decimal.Parse(sourceValue.Replace(" STEEM", "").Replace(".", ","));
        }

        private class TransactionsResp
        {
            public dynamic[][] Result { get; set; }
        }
    }
}