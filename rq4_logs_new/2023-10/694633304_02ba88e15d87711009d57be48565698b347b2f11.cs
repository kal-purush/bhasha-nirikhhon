using AutoMapper;
using ChoirManager.Business.Abstractions;
using ChoirManager.Business.DTOs.ChoirDtos;
using ChoirManager.Business.Shared;
using ChoirManager.Core.Abstractions.QueryOptions;
using ChoirManager.Core.Abstractions.Repositories;
using ChoirManager.Core.CoreEntities;
using ChoirManager.Core.QueryOptions;

namespace ChoirManager.Business.Services;

public class ChoirService : IChoirService
{
    private readonly IChoirRepository _choirRepository;
    private readonly IMapper _mapper;

    public ChoirService(IChoirRepository repository, IMapper mapper)
    {
        _choirRepository = repository;
        _mapper = mapper;
    }
    
    public async Task<ChoirGetDto> GetOneAsync(string altKey)
    {
        return _mapper.Map<ChoirGetDto>(await _choirRepository.GetOneAsync(altKey));
    }

    public async Task<ChoirGetDto[]> GetManyAsync(IQueryOptions queryOptions)
    {
        var choirQueryOptions = (ChoirQueryOptions)queryOptions;
        return _mapper.Map<ChoirGetDto[]>(await _choirRepository.GetAllAsync(choirQueryOptions));
    }

    public async Task<ChoirGetDto> CreateOneAsync(ChoirCreateDto createDto)
    {
        var choirEntity = _mapper.Map<Choir>(createDto);
        return _mapper.Map<ChoirGetDto>(await _choirRepository.CreateOneAsync(choirEntity));
    }

    public async Task<ChoirGetDto> UpdateOneAsync(ChoirUpdateDto updateDto)
    {
        var updateEntity = _mapper.Map<Choir>(updateDto);
        var original = await _choirRepository.GetOneAsync(updateEntity.Name);
        EntityHelper<Choir>.CheckNullValues(original, updateEntity);
        EntityHelper<Choir>.ReplaceProperyValues(original, updateEntity);
        return _mapper.Map<ChoirGetDto>(await _choirRepository.UpdateAsync(original));
    }

    public async Task<bool> RemoveOne(string altKey)
    {
        var entity = await _choirRepository.GetOneAsync(altKey);
        return await _choirRepository.RemoveAsync(entity);
    }
}