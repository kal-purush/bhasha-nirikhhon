using System.Threading.Tasks;
using AwakenServer.StatInfo.Dtos;
using AwakenServer.Trade.Dtos;
using Volo.Abp.Application.Dtos;
using Volo.Abp.Application.Services;

namespace AwakenServer.StatInfo;

public interface IStatInfoAppService : IApplicationService
{
    Task<ListResultDto<StatInfoTvlDto>> GetTvlListAsync(GetStatHistoryInput input);
    Task<ListResultDto<StatInfoPriceDto>> GetPriceListAsync(GetStatHistoryInput input);
    Task<ListResultDto<StatInfoVolumeDto>> GetVolumeListAsync(GetStatHistoryInput input);
}