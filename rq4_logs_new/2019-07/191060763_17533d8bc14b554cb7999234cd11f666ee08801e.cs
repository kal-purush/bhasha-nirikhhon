using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.BlockchainBalancesReport.Clients.Samurai;
using Lykke.Job.BlockchainBalancesReport.Settings;
using Lykke.Job.BlockchainBalancesReport.Utils;
using Lykke.Service.Assets.Client;

namespace Lykke.Job.BlockchainBalancesReport.Blockchains.Ethereum
{
    public class EthereumClassicBalanceProvider : 
        IBalanceProvider,
        IAsyncInitialization
    {
        public Task AsyncInitialization { get; }
        public string BlockchainType => "EthereumClassic";

        private readonly SamuraiBalanceProvider _balanceProvider;

        public EthereumClassicBalanceProvider(
            EthereumClassicSettings settings,
            IAssetsServiceWithCache assetsServiceClient)
        {
            _balanceProvider = new SamuraiBalanceProvider
            (
                settings.SamuraiApiUrl,
                new BlockchainAsset("ETC", "ETC", "915c4074-ec20-40ed-b8b7-5e3cc2f303b1"),
                assetsServiceClient
            );

            AsyncInitialization = InitializeAsync();
        }

        public async Task<IReadOnlyDictionary<BlockchainAsset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            return await _balanceProvider.GetBalancesAsync(address, at);
        }

        private async Task InitializeAsync()
        {
            await _balanceProvider.AsyncInitialization;
        }
    }
}