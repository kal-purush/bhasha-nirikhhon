using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AElf.Indexing.Elasticsearch;
using Awaken.Contracts.Hooks;
using AwakenServer.Grains.Tests;
using AwakenServer.Provider;
using AwakenServer.Trade;
using AwakenServer.Trade.Dtos;
using Google.Protobuf;
using Microsoft.AspNetCore.Routing.Matching;
using Nest;
using Shouldly;
using Volo.Abp.EventBus.Local;
using Xunit;
using Token = AwakenServer.Tokens.Token;

namespace AwakenServer.Activity
{
    [Collection(ClusterCollection.Name)]
    public class ActivityAppServiceDataTests : TradeTestBase
    {
        private readonly IActivityAppService _activityAppService;

        public ActivityAppServiceDataTests()
        {
            _activityAppService = GetRequiredService<IActivityAppService>();
        }

        [Fact]
        public async Task SwapTest()
        {
            var swapRecordDto = new SwapRecordDto
            {
                ChainId = ChainId,
                PairAddress = TradePairEthUsdtAddress,
                Sender = "TV2aRV4W5oSJzxrkBvj8XmJKkMCiEQnAvLmtM9BqLTN3beXm2",
                TransactionHash = "6622966a928185655d691565d6128835e7d1ccdf1dd3b5f277c5f2a5b2802d37",
                Timestamp = DateTimeHelper.ToUnixTimeMilliseconds(DateTime.UtcNow),
                AmountOut = NumberFormatter.WithDecimals(10, 8),
                AmountIn = NumberFormatter.WithDecimals(100, 6),
                SymbolIn = TokenUsdtSymbol,
                SymbolOut = TokenEthSymbol,
                Channel = "test",
                BlockHeight = 99,
            };
            await _activityAppService.CreateSwapAsync(swapRecordDto);
        }
        
    }
}