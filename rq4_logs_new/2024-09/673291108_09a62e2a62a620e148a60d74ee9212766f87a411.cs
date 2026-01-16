using AwakenServer.Grains.State.Activity;
using Orleans;
using Volo.Abp.ObjectMapping;

namespace AwakenServer.Grains.Grain.Activity;

public class JoinRecordGrain : Grain<JoinRecordState>, IJoinRecordGrain
{
    private readonly IObjectMapper _objectMapper;

    public JoinRecordGrain(IObjectMapper objectMapper)
    {
        _objectMapper = objectMapper;
    }

    public async Task<GrainResultDto<JoinRecordGrainDto>> GetAsync()
    {
        await ReadStateAsync();
        if (string.IsNullOrEmpty(State.Address))
        {
            return new GrainResultDto<JoinRecordGrainDto>
            {
                Success = false
            };
        }

        return new GrainResultDto<JoinRecordGrainDto>
        {
            Success = true,
            Data = _objectMapper.Map<JoinRecordState, JoinRecordGrainDto>(State)
        };
    }

    public async Task<GrainResultDto<JoinRecordGrainDto>> AddOrUpdateAsync(JoinRecordGrainDto grainDto)
    {
        if (!string.IsNullOrEmpty(State.Address) && State.Address == grainDto.Address)
        {
            return new GrainResultDto<JoinRecordGrainDto>
            {
                Success = false
            };
        }

        _objectMapper.Map(grainDto, State);
        await WriteStateAsync();
        return new GrainResultDto<JoinRecordGrainDto>()
        {
            Success = true,
            Data = _objectMapper.Map<JoinRecordState, JoinRecordGrainDto>(State)
        };
    }
}