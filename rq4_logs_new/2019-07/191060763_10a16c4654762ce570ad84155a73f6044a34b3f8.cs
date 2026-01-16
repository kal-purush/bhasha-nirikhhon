using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Tools.BlockchainBalancesReport.Blockchains;
using Lykke.Tools.BlockchainBalancesReport.Blockchains.Decred;
using Lykke.Tools.BlockchainBalancesReport.Blockchains.Neo;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Tests
{
    public class DecredProviderTests
    {
        [Fact]
        public async Task CanCalculateBalanceAtPointOfTime()
        {
            var serviceProvider = new ServiceCollection()
                .AddLogging()
                .BuildServiceProvider();

            var balanceProvider = new DecredBalanceProvider(serviceProvider.GetService<ILoggerFactory>(), 
                "https://explorer.dcrdata.org/insight/api/" );

            var decredAsset = new Asset("DCR","DCR", "02154b48-7ed9-4211-b614-e87679fd4f5a");
            var expectations = new List<(string address, DateTime dateTime, IReadOnlyDictionary<Asset, decimal> result)>
            {
                ("DscTMvxiYJvUsC9VFFTLTFj9MPWo2Togc33", DateTime.Parse("2019-07-01T19:00:00+0000"), new Dictionary<Asset, decimal>
                {
                    {decredAsset, 100.51172068m}
                }),
            };

            foreach (var assert in expectations)
            {
                var result = await balanceProvider.GetBalancesAsync(assert.address, assert.dateTime);

                Assert.Equal(result, assert.result);
            }
        }
    }
}