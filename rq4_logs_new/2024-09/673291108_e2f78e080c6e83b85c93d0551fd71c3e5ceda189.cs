using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AElf.Indexing.Elasticsearch;
using AwakenServer.Grains;
using AwakenServer.Grains.Grain.StatInfo;
using AwakenServer.StatInfo;
using AwakenServer.StatInfo.Etos;
using AwakenServer.Trade;
using AwakenServer.Trade.Dtos;
using Microsoft.Extensions.Caching.Distributed;
using Nest;
using Orleans;
using Volo.Abp.Application.Services;
using Volo.Abp.Caching;
using Volo.Abp.EventBus.Distributed;
using SwapRecord = AwakenServer.Trade.SwapRecord;
using TradePair = AwakenServer.Trade.Index.TradePair;

namespace AwakenServer.DataInfo;

public class StatInfoAppService : ApplicationService
{
    public const string SyncedTransactionCachePrefix = "StatInfoSynced";
    private readonly IClusterClient _clusterClient;
    private readonly IDistributedEventBus _distributedEventBus;
    private readonly INESTRepository<TradePair, Guid> _tradePairIndexRepository;
    private readonly ITokenPriceProvider _tokenPriceProvider;
    private readonly IDistributedCache<string> _syncedTransactionIdCache;
    
    private string AddVersionToKey(string baseKey, string version)
    {
        return $"{baseKey}:{version}";
    }
    public async Task<bool> CreateLiquidityRecordAsync(LiquidityRecordDto liquidityRecordDto, string dataVersion)
    {
        var key = AddVersionToKey($"{SyncedTransactionCachePrefix}:{nameof(LiquidityRecord)}:{liquidityRecordDto.TransactionHash}", dataVersion);
        var existed = await _syncedTransactionIdCache.GetAsync(key);
        if (!existed.IsNullOrWhiteSpace())
        {
            return false;
        }
        // insert transaction
        var tradePair = await GetTradePairAsync(liquidityRecordDto.ChainId, liquidityRecordDto.Pair);
        if (tradePair == null)
        {
            return false;
        }
        var transactionHistory = new TransactionHistoryEto()
        {
            TradePair = tradePair,
            TransactionType = liquidityRecordDto.Type == LiquidityType.Mint ? TransactionType.Add : TransactionType.Remove,
            TransactionHash = liquidityRecordDto.TransactionHash,
            Timestamp = liquidityRecordDto.Timestamp,
            ChainId = liquidityRecordDto.ChainId
        };
        var isTokenReversed = tradePair.Token0.Symbol != liquidityRecordDto.Token0;
        transactionHistory.Token0Amount = isTokenReversed
            ? liquidityRecordDto.Token1Amount.ToDecimalsString(tradePair.Token0.Decimals)
            : liquidityRecordDto.Token0Amount.ToDecimalsString(tradePair.Token0.Decimals);
        transactionHistory.Token1Amount = isTokenReversed
            ? liquidityRecordDto.Token0Amount.ToDecimalsString(tradePair.Token1.Decimals)
            : liquidityRecordDto.Token1Amount.ToDecimalsString(tradePair.Token1.Decimals);
        var priceTuple = await _tokenPriceProvider.GetUSDPriceAsync(liquidityRecordDto.ChainId, tradePair.Id, tradePair.Token0.Symbol, tradePair.Token1.Symbol);
        transactionHistory.ValueInUsd = priceTuple.Item1 * Double.Parse(transactionHistory.Token0Amount) +
                                        priceTuple.Item2 * Double.Parse(transactionHistory.Token1Amount);
        transactionHistory.Version = dataVersion;
        await _distributedEventBus.PublishAsync(transactionHistory);
        
        // transaction count
        await IncTransactionCount(tradePair.ChainId, tradePair.Address, tradePair.Token0.Symbol,
            tradePair.Token1.Symbol);
        
        await _syncedTransactionIdCache.SetAsync(key, "1", new DistributedCacheEntryOptions
        {
            AbsoluteExpiration = DateTimeOffset.UtcNow.AddDays(7)
        });
        return true;
    }
    
    public async Task<bool> CreateSwapRecordAsync(SwapRecordDto swapRecordDto, string dataVersion)
    {
        var key = AddVersionToKey($"{SyncedTransactionCachePrefix}:{nameof(SwapRecord)}:{swapRecordDto.TransactionHash}", dataVersion);
        var existed = await _syncedTransactionIdCache.GetAsync(key);
        if (!existed.IsNullOrWhiteSpace())
        {
            return false;
        }
        await SyncSingleSwapRecordAsync(swapRecordDto, dataVersion);
        if (!swapRecordDto.SwapRecords.IsNullOrEmpty())
        {
            foreach (var swapRecord in swapRecordDto.SwapRecords)
            {
                ObjectMapper.Map(swapRecord, swapRecordDto);
                await SyncSingleSwapRecordAsync(swapRecordDto, dataVersion);
            }
        }
        await _syncedTransactionIdCache.SetAsync(key, "1");
        return true;
    }

    public async Task<bool> SyncSingleSwapRecordAsync(SwapRecordDto swapRecordDto, string dataVersion)
    {
        // insert transaction
        var tradePair = await GetTradePairAsync(swapRecordDto.ChainId, swapRecordDto.PairAddress);
        if (tradePair == null)
        {
            return false;
        }

        var isSell = swapRecordDto.SymbolIn == tradePair.Token0.Symbol;
        var transactionHistory = new TransactionHistoryEto()
        {
            TradePair = tradePair,
            TransactionType = TransactionType.Trade,
            TransactionHash = swapRecordDto.TransactionHash,
            Timestamp = swapRecordDto.Timestamp,
            ChainId = swapRecordDto.ChainId,
            Side = isSell ? TradeSide.Sell : TradeSide.Buy
        };
        transactionHistory.Token0Amount = isSell
            ? swapRecordDto.AmountIn.ToDecimalsString(tradePair.Token0.Decimals)
            : swapRecordDto.AmountOut.ToDecimalsString(tradePair.Token0.Decimals);
        transactionHistory.Token1Amount = isSell 
            ? swapRecordDto.AmountOut.ToDecimalsString(tradePair.Token1.Decimals)
            : swapRecordDto.AmountIn.ToDecimalsString(tradePair.Token1.Decimals);
        var token0Price = await _tokenPriceProvider.GetTokenUSDPriceAsync(
            swapRecordDto.ChainId, tradePair.Token0.Symbol);
        transactionHistory.ValueInUsd = token0Price * Double.Parse(transactionHistory.Token0Amount);
        transactionHistory.Version = dataVersion;
        
        // pool lpFee/volume
        var poolStatInfoGrain = _clusterClient.GetGrain<IPoolStatInfoGrain>(GrainIdHelper.GenerateGrainId(swapRecordDto.ChainId, swapRecordDto.PairAddress));
        var poolStatInfoGrainDtoResult = await poolStatInfoGrain.GetAsync();
        poolStatInfoGrainDtoResult.Data.VolumeInUsd7d = 0;//todo
        poolStatInfoGrainDtoResult.Data.VolumeInUsd24h = 0;//todo
        poolStatInfoGrainDtoResult.Data.TransactionCount++;
        await poolStatInfoGrain.AddOrUpdateAsync(poolStatInfoGrainDtoResult.Data);
        await _distributedEventBus.PublishAsync(
            ObjectMapper.Map<PoolStatInfoGrainDto, PoolStatInfoEto>(poolStatInfoGrainDtoResult.Data));
        
        var token1Price = await _tokenPriceProvider.GetTokenUSDPriceAsync(
            swapRecordDto.ChainId, tradePair.Token1.Symbol);
        var lpFeeAmount = swapRecordDto.TotalFee.ToDecimalsString(isSell ? tradePair.Token0.Decimals : tradePair.Token1.Decimals);
        var snapshotEto = new StatInfoSnapshotEto
        {
            StatType = 2,
            PairAddress = tradePair.Address,
            Timestamp = swapRecordDto.Timestamp,
            VolumeInUsd = transactionHistory.ValueInUsd,
            LpFeeInUsd = Double.Parse(lpFeeAmount) * (isSell ? token0Price : token1Price)
        };
        await _distributedEventBus.PublishAsync(snapshotEto);

        // token volume
        await UpdateTokenVolume(swapRecordDto.ChainId, tradePair.Token0.Symbol, 
            transactionHistory.ValueInUsd, swapRecordDto.Timestamp);
        await UpdateTokenVolume(swapRecordDto.ChainId, tradePair.Token1.Symbol, 
            Double.Parse(transactionHistory.Token1Amount) * token1Price, swapRecordDto.Timestamp);
        
        // global volume
        var globalSnapshotEto = new StatInfoSnapshotEto
        {
            StatType = 0,
            Timestamp = swapRecordDto.Timestamp,
            VolumeInUsd = transactionHistory.ValueInUsd,
        };
        await _distributedEventBus.PublishAsync(globalSnapshotEto);
        return true;
    }

    public async Task<bool> CreateSyncRecordAsync(SyncRecordDto syncRecordDto, string dataVersion)
    {
        var key = AddVersionToKey($"{SyncedTransactionCachePrefix}:{nameof(syncRecordDto)}:{syncRecordDto.TransactionHash}", dataVersion);
        var existed = await _syncedTransactionIdCache.GetAsync(key);
        if (!existed.IsNullOrWhiteSpace())
        {
            return false;
        }
        var tradePair = await GetTradePairAsync(syncRecordDto.ChainId, syncRecordDto.PairAddress);
        if (tradePair == null)
        {
            return false;
        }
        var isTokenReversed = tradePair.Token0.Symbol != syncRecordDto.SymbolA;
        var token0Amount = isTokenReversed
            ? syncRecordDto.ReserveB.ToDecimalsString(tradePair.Token0.Decimals)
            : syncRecordDto.ReserveA.ToDecimalsString(tradePair.Token0.Decimals);
        var token1Amount = isTokenReversed
            ? syncRecordDto.ReserveA.ToDecimalsString(tradePair.Token1.Decimals)
            : syncRecordDto.ReserveB.ToDecimalsString(tradePair.Token1.Decimals);
        var (token0Price, token1Price) = await _tokenPriceProvider.GetUSDPriceAsync(syncRecordDto.ChainId, tradePair.Id,
            tradePair.Token0.Symbol, tradePair.Token1.Symbol);
        
        // pool tvl/price
        var poolStateInfoGrain = _clusterClient.GetGrain<IPoolStatInfoGrain>(GrainIdHelper.GenerateGrainId(syncRecordDto.ChainId, syncRecordDto.PairAddress));
        var poolStateInfoGrainDtoResult = await poolStateInfoGrain.GetAsync();
        var oldValueLocked0 = poolStateInfoGrainDtoResult.Data.ValueLocked0;
        var oldValueLocked1 = poolStateInfoGrainDtoResult.Data.ValueLocked1;
        var oldTvl = poolStateInfoGrainDtoResult.Data.Tvl;
        
        poolStateInfoGrainDtoResult.Data.PairAddress = tradePair.Address;
        poolStateInfoGrainDtoResult.Data.ValueLocked0 = double.Parse(token0Amount);
        poolStateInfoGrainDtoResult.Data.ValueLocked1 = double.Parse(token1Amount);
        poolStateInfoGrainDtoResult.Data.Tvl =
            double.Parse(token0Amount) * token0Price + double.Parse(token1Amount) * token1Price;
        poolStateInfoGrainDtoResult.Data.LastUpdateTime = DateTimeHelper.FromUnixTimeMilliseconds(syncRecordDto.Timestamp);
        await poolStateInfoGrain.AddOrUpdateAsync(poolStateInfoGrainDtoResult.Data);
        await _distributedEventBus.PublishAsync(
            ObjectMapper.Map<PoolStatInfoGrainDto, PoolStatInfoEto>(poolStateInfoGrainDtoResult.Data));

        var poolSnapshotEto = new StatInfoSnapshotEto()
        {
            StatType = 2,
            PairAddress = syncRecordDto.PairAddress,
            Price = double.Parse(token1Amount) / double.Parse(token0Amount),
            Tvl = poolStateInfoGrainDtoResult.Data.Tvl,
            Timestamp = syncRecordDto.Timestamp
        };
        await _distributedEventBus.PublishAsync(poolSnapshotEto);
        
        // follow token price/tvl
        var tokenStatInfoGrain = _clusterClient.GetGrain<ITokenStatInfoGrain>(
            GrainIdHelper.GenerateGrainId(syncRecordDto.ChainId, tradePair.Token0.Symbol));
        var tokenStatInfoGrainDtoResult = await tokenStatInfoGrain.GetAsync();
        tokenStatInfoGrainDtoResult.Data.Symbol = tradePair.Token0.Symbol;
        tokenStatInfoGrainDtoResult.Data.ValueLocked += double.Parse(token0Amount) - oldValueLocked0;
        tokenStatInfoGrainDtoResult.Data.Tvl = tokenStatInfoGrainDtoResult.Data.ValueLocked * token0Price;
        tokenStatInfoGrainDtoResult.Data.LastUpdateTime = DateTimeHelper.FromUnixTimeMilliseconds(syncRecordDto.Timestamp);
        tokenStatInfoGrainDtoResult.Data.Price = 0; //todo
        await tokenStatInfoGrain.AddOrUpdateAsync(tokenStatInfoGrainDtoResult.Data);
        await _distributedEventBus.PublishAsync(
            ObjectMapper.Map<TokenStatInfoGrainDto, TokenStatInfoEto>(tokenStatInfoGrainDtoResult.Data));
        
        var token0SnapshotEto = new StatInfoSnapshotEto()
        {
            StatType = 1,
            Symbol = tradePair.Token0.Symbol,
            Price = 0, // todo
            Tvl = tokenStatInfoGrainDtoResult.Data.Tvl,
            Timestamp = syncRecordDto.Timestamp
        };
        await _distributedEventBus.PublishAsync(token0SnapshotEto);
        
        var token1StatInfoGrain = _clusterClient.GetGrain<ITokenStatInfoGrain>(
            GrainIdHelper.GenerateGrainId(syncRecordDto.ChainId, tradePair.Token1.Symbol));
        var token1StatInfoGrainDtoResult = await token1StatInfoGrain.GetAsync();
        token1StatInfoGrainDtoResult.Data.Symbol = tradePair.Token1.Symbol;
        token1StatInfoGrainDtoResult.Data.ValueLocked += double.Parse(token1Amount) - oldValueLocked1;
        token1StatInfoGrainDtoResult.Data.Tvl = token1StatInfoGrainDtoResult.Data.ValueLocked * token1Price;
        token1StatInfoGrainDtoResult.Data.LastUpdateTime = DateTimeHelper.FromUnixTimeMilliseconds(syncRecordDto.Timestamp);
        token1StatInfoGrainDtoResult.Data.Price = 0; //todo
        await token1StatInfoGrain.AddOrUpdateAsync(token1StatInfoGrainDtoResult.Data);
        
        await _distributedEventBus.PublishAsync(
            ObjectMapper.Map<TokenStatInfoGrainDto, TokenStatInfoEto>(token1StatInfoGrainDtoResult.Data));
        
        var token1SnapshotEto = new StatInfoSnapshotEto()
        {
            StatType = 1,
            Symbol = tradePair.Token1.Symbol,
            Price = double.Parse(token1Amount) / double.Parse(token0Amount),
            Tvl = poolStateInfoGrainDtoResult.Data.Tvl,
            Timestamp = syncRecordDto.Timestamp
        };
        await _distributedEventBus.PublishAsync(token1SnapshotEto);
        // global tvl
        var globalGrain = _clusterClient.GetGrain<IGlobalStatInfoGrain>(syncRecordDto.ChainId);
        var globalGrainDtoResult = await globalGrain.AddTvlAsync(poolStateInfoGrainDtoResult.Data.Tvl - oldTvl);
        var globalSnapshotEto = new StatInfoSnapshotEto()
        {
            StatType = 0,
            Tvl = globalGrainDtoResult.Data.Tvl,
            Timestamp = syncRecordDto.Timestamp
        };
        await _distributedEventBus.PublishAsync(globalSnapshotEto);
        
        await _syncedTransactionIdCache.SetAsync(key, "1");
        return true;
    }

    public async void RefreshTvlAsync()
    {
        // refresh token tvl
        // refresh global tvl
    }

    public async void InitTokenFollowPairAsync()
    {
        
    }

    private async Task IncTransactionCount(string chainId, string pairAddress, string symbol0, string symbol1)
    {
        var poolStatInfoGrain = _clusterClient.GetGrain<IPoolStatInfoGrain>(GrainIdHelper.GenerateGrainId(chainId, pairAddress));
        var poolStatInfoGrainDtoResult = await poolStatInfoGrain.GetAsync();
        poolStatInfoGrainDtoResult.Data.TransactionCount++;
        await poolStatInfoGrain.AddOrUpdateAsync(poolStatInfoGrainDtoResult.Data);
        await _distributedEventBus.PublishAsync(ObjectMapper.Map<PoolStatInfoGrainDto, PoolStatInfoEto>(poolStatInfoGrainDtoResult.Data));
        
        await IncTokenTransactionCount(chainId, symbol0);
        await IncTokenTransactionCount(chainId, symbol1);
    }

    private async Task IncTokenTransactionCount(string chainId, string symbol)
    {
        var tokenStatInfoGrain = _clusterClient.GetGrain<ITokenStatInfoGrain>(GrainIdHelper.GenerateGrainId(chainId, symbol));
        var tokenStatInfoGrainDtoResult = await tokenStatInfoGrain.GetAsync();
        tokenStatInfoGrainDtoResult.Data.TransactionCount++;
        await tokenStatInfoGrain.AddOrUpdateAsync(tokenStatInfoGrainDtoResult.Data);
        await _distributedEventBus.PublishAsync(ObjectMapper.Map<TokenStatInfoGrainDto, TokenStatInfoEto>(tokenStatInfoGrainDtoResult.Data));
    }

    private async Task UpdateTokenVolume(string chainId, string symbol, double volumeInUsd, long timestamp)
    {
        var tokenStatInfoGrain = _clusterClient.GetGrain<ITokenStatInfoGrain>(GrainIdHelper.GenerateGrainId(chainId, symbol));
        var tokenStatInfoGrainDtoResult = await tokenStatInfoGrain.GetAsync();
        tokenStatInfoGrainDtoResult.Data.TransactionCount++;
        tokenStatInfoGrainDtoResult.Data.VolumeInUsd24h = 0;
        await tokenStatInfoGrain.AddOrUpdateAsync(tokenStatInfoGrainDtoResult.Data);
        await _distributedEventBus.PublishAsync(ObjectMapper.Map<TokenStatInfoGrainDto, TokenStatInfoEto>(tokenStatInfoGrainDtoResult.Data));
        
        var tokenSnapshotEto = new StatInfoSnapshotEto
        {
            StatType = 1,
            Symbol = symbol,
            Timestamp = timestamp,
            VolumeInUsd = volumeInUsd,
        };
        await _distributedEventBus.PublishAsync(tokenSnapshotEto);
    }

    public async Task<TradePair> GetTradePairAsync(string chainName, string address)
    {
        var mustQuery = new List<Func<QueryContainerDescriptor<TradePair>, QueryContainer>>();
        mustQuery.Add(q => q.Term(i => i.Field(f => f.ChainId).Value(chainName)));
        mustQuery.Add(q => q.Term(i => i.Field(f => f.Address).Value(address)));

        QueryContainer Filter(QueryContainerDescriptor<TradePair> f) => f.Bool(b => b.Must(mustQuery));
        return await _tradePairIndexRepository.GetAsync(Filter);
    }
}