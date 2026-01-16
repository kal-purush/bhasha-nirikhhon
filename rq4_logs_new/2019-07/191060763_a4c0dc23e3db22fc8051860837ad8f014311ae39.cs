using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Tools.BlockchainBalancesReport.Clients.NeoScan;
using Lykke.Tools.BlockchainBalancesReport.Configuration;
using Microsoft.Extensions.Options;

namespace Lykke.Tools.BlockchainBalancesReport.Blockchains.Neo
{
    public class NeoBalanceProvider: IBalanceProvider
    {
        private readonly NeoScanClient _neoScanClient;

        public NeoBalanceProvider(IOptions<NeoSettings> settings)
        {
            _neoScanClient = new NeoScanClient(settings.Value.NeoScanBaseUrl);
        }

        public string BlockchainType => "NEO";

        public  async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            var balances =await _neoScanClient.GetBalanceAsync(address, at);

            return balances.ToDictionary(p => BuildAsset(p.Key), p => p.Value);
        }

        private Asset BuildAsset(string blockchainAssetName)
        {
            string lykkeAssetId;
            switch (blockchainAssetName)
            {
                case "NEO":
                {
                    lykkeAssetId = "ac2e579f-187b-4429-8d60-bea6e4f65f76";
                    break;
                }
                case "GAS":
                {
                    lykkeAssetId = "f1ccf1dd-9008-4999-adc8-2cb587717083";
                    break;
                }
                default:
                {
                    lykkeAssetId = null;
                    break;
                }
            }

            return new Asset(blockchainAssetName, "Neo", lykkeAssetId);
        } 
    }
}