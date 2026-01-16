using System;
using System.Threading.Tasks;
using AElf.Indexing.Elasticsearch;
using AwakenServer.Grains;
using AwakenServer.Grains.Grain.MyPortfolio;
using AwakenServer.Grains.Grain.StatInfo;
using AwakenServer.Grains.Tests;
using AwakenServer.StatInfo.Index;
using AwakenServer.Trade;
using AwakenServer.Trade.Dtos;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;
using TradePair = AwakenServer.Trade.Index.TradePair;

namespace AwakenServer.StatInfo;

[Collection(ClusterCollection.Name)]
public class StatInfoInternalAppServiceTests : TradeTestBase
{
    private readonly IStatInfoInternalAppService _statInfoInternalAppService;
    private readonly INESTRepository<TradePair, Guid> _tradePairIndexRepository;
    private readonly INESTRepository<TokenStatInfoIndex, Guid> _tokenStatInfoIndexRepository;
    private readonly INESTRepository<PoolStatInfoIndex, Guid> _poolStatInfoIndexRepository;
    private readonly INESTRepository<TransactionHistoryIndex, Guid> _transactionIndexRepository;
    private readonly INESTRepository<StatInfoSnapshotIndex, Guid> _statInfoSnapshotIndexRepository;
    private readonly IOptionsSnapshot<StatInfoOptions> _statInfoOptions;

    public StatInfoInternalAppServiceTests()
    {
        _statInfoInternalAppService = GetRequiredService<IStatInfoInternalAppService>();
        _tradePairIndexRepository = GetRequiredService<INESTRepository<TradePair, Guid>>();
        _tokenStatInfoIndexRepository = GetRequiredService<INESTRepository<TokenStatInfoIndex, Guid>>();
        _poolStatInfoIndexRepository = GetRequiredService<INESTRepository<PoolStatInfoIndex, Guid>>();
        _transactionIndexRepository = GetRequiredService<INESTRepository<TransactionHistoryIndex, Guid>>();
        _statInfoSnapshotIndexRepository = GetRequiredService<INESTRepository<StatInfoSnapshotIndex, Guid>>();
        _statInfoOptions = GetRequiredService<IOptionsSnapshot<StatInfoOptions>>();
    }
    private string AddVersionToKey(string baseKey, string version)
    {
        return $"{baseKey}:{version}";
    }
    
    [Fact]
    public async Task SyncAddLiquidityRecordTest()
    {
        var inputMint = new LiquidityRecordDto()
        {
            ChainId = ChainName,
            Pair = TradePairBtcUsdtAddress,
            Address = "0x123456789",
            Timestamp = 1000,
            Token0Amount = 100,
            Token0 = "BTC",
            Token1Amount = 1000,
            Token1 = "USDT",
            LpTokenAmount = 50000,
            Type = LiquidityType.Mint,
            TransactionHash = "0xdab24d0f0c28a3be6b59332ab0cb0b4cd54f10f3c1b12cfc81d72e934d74b28f",
            Channel = "TestChanel",
            Sender = "0x123456789",
            To = "0x123456789",
            BlockHeight = 100
        };
        var syncResult = await _statInfoInternalAppService.CreateLiquidityRecordAsync(inputMint, _statInfoOptions.Value.DataVersion);
        syncResult.ShouldBeTrue();

        // pool
        var poolStatInfoGrain =
            Cluster.Client.GetGrain<IPoolStatInfoGrain>(AddVersionToKey(TradePairBtcUsdtAddress, _statInfoOptions.Value.DataVersion));
        var poolStatInfoResult = await poolStatInfoGrain.GetAsync();
        poolStatInfoResult.Success.ShouldBeTrue();
        poolStatInfoResult.Data.PairAddress.ShouldBe(TradePairBtcUsdtAddress);
        poolStatInfoResult.Data.TransactionCount.ShouldBe(1);
        poolStatInfoResult.Data.LastUpdateTime.ShouldBe(1000);
        
        var poolStatInfoIndex = await _poolStatInfoIndexRepository.GetAsync(q =>
            q.Term(i => i.Field(f => f.PairAddress).Value(TradePairBtcUsdtAddress)) &&
            q.Term(i => i.Field(f => f.Version).Value(_statInfoOptions.Value.DataVersion)));
        poolStatInfoIndex.TransactionCount.ShouldBe(1);
        poolStatInfoIndex.PairAddress.ShouldBe(TradePairBtcUsdtAddress);
        poolStatInfoIndex.TradePair.Id.ShouldBe(TradePairBtcUsdtId);
        poolStatInfoIndex.LastUpdateTime.ShouldBe(1000);

       // token0
       var token0StatInfoGrain =
           Cluster.Client.GetGrain<ITokenStatInfoGrain>(AddVersionToKey(inputMint.Token0, _statInfoOptions.Value.DataVersion));
       var token0StatInfoResult = await token0StatInfoGrain.GetAsync();
       token0StatInfoResult.Success.ShouldBeTrue();
       token0StatInfoResult.Data.Symbol.ShouldBe(inputMint.Token0);
       token0StatInfoResult.Data.TransactionCount.ShouldBe(1);
       token0StatInfoResult.Data.LastUpdateTime.ShouldBe(1000);
        
       var token0StatInfoIndex = await _tokenStatInfoIndexRepository.GetAsync(q =>
           q.Term(i => i.Field(f => f.Symbol).Value(inputMint.Token0)) &&
           q.Term(i => i.Field(f => f.ChainId).Value(inputMint.ChainId)));
       token0StatInfoIndex.TransactionCount.ShouldBe(1);
       token0StatInfoIndex.Symbol.ShouldBe(inputMint.Token0);
       token0StatInfoIndex.TransactionCount.ShouldBe(1);
       token0StatInfoIndex.LastUpdateTime.ShouldBe(1000);
       
       // token1
       var token1StatInfoGrain =
           Cluster.Client.GetGrain<ITokenStatInfoGrain>(AddVersionToKey(inputMint.Token1, _statInfoOptions.Value.DataVersion));
       var token1StatInfoResult = await token1StatInfoGrain.GetAsync();
       token1StatInfoResult.Success.ShouldBeTrue();
       token1StatInfoResult.Data.Symbol.ShouldBe(inputMint.Token1);
       token1StatInfoResult.Data.TransactionCount.ShouldBe(1);
       token1StatInfoResult.Data.LastUpdateTime.ShouldBe(1000);
        
       var token1StatInfoIndex = await _tokenStatInfoIndexRepository.GetAsync(q =>
           q.Term(i => i.Field(f => f.Symbol).Value(inputMint.Token1)) &&
           q.Term(i => i.Field(f => f.ChainId).Value(inputMint.ChainId)));
       token1StatInfoIndex.TransactionCount.ShouldBe(1);
       token1StatInfoIndex.Symbol.ShouldBe(inputMint.Token1);
       token1StatInfoIndex.TransactionCount.ShouldBe(1);
       token1StatInfoIndex.LastUpdateTime.ShouldBe(1000);
       
       // transaction history
       var transactionHistoryIndex = await _transactionIndexRepository.GetAsync(q =>
           q.Term(i => i.Field(f => f.TransactionHash).Value(inputMint.TransactionHash)) &&
           q.Term(i => i.Field(f => f.ChainId).Value(inputMint.ChainId)) &&
           q.Term(i => i.Field(f => f.Version).Value(_statInfoOptions.Value.DataVersion)));
       transactionHistoryIndex.TradePair.Id.ShouldBe(TradePairBtcUsdtId);
       transactionHistoryIndex.TransactionType.ShouldBe(TransactionType.Add);
       transactionHistoryIndex.Token0Amount.ShouldBe(inputMint.Token0Amount.ToDecimalsString(8));
       transactionHistoryIndex.Token1Amount.ShouldBe(inputMint.Token1Amount.ToDecimalsString(6));
       transactionHistoryIndex.Timestamp.ShouldBe(1000);
       transactionHistoryIndex.ValueInUsd.ShouldBe(double.Parse(transactionHistoryIndex.Token0Amount) + double.Parse(transactionHistoryIndex.Token1Amount));
    }
}