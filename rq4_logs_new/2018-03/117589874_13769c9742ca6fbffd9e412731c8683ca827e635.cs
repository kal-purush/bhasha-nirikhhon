using System.Collections.Generic;
using System.Linq;
using NBitcoin;
using QBitNinja.Client.Models;

namespace Lykke.Job.PayTransactionHandler.Services
{
    public static class BitcoinExtensions
    {
        public static BitcoinAddress GetDestinationMainAddress(this ICoin src)
        {
            return src?.TxOut?.ScriptPubKey?.GetDestinationAddress(Network.Main);
        }

        public static IEnumerable<BitcoinAddress> GetSourceWalletAddresses(this GetTransactionResponse src)
        {
            return src?.SpentCoins?.Select(x => x.GetDestinationMainAddress()).Where(x => x != null);
        }
    }
}