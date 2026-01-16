using System.Threading.Tasks;
using AwakenServer.Trade.Dtos;

namespace AwakenServer.StatInfo;

public interface IStatInfoInternalAppService
{
    Task<bool> CreateLiquidityRecordAsync(LiquidityRecordDto liquidityRecordDto, string dataVersion);
    Task<bool> CreateSwapRecordAsync(SwapRecordDto swapRecordDto, string dataVersion);
    Task<bool> CreateSyncRecordAsync(SyncRecordDto syncRecordDto, string dataVersion);
    Task RefreshTvlAsync(string chainId, string dataVersion);
    Task UpdateTokenFollowPairAsync(string chainId, string dataVersion);
    Task ClearOldTransactionHistoryAsync(string chainId, string dataVersion);
}