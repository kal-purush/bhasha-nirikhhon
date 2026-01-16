using AutoMapper;
using Backend.Application.Context;
using MediatR;

namespace Backend.Application.Beach.CreateBeach;

public class CreateBeachCommandHandler : IRequestHandler<CreateBeachRequest, int>
{
    private readonly IDGDBContext _myWorldDbContext;
    private readonly IMapper _mapper;
    public CreateBeachCommandHandler(IDGDBContext myWorldDbContext,
    IMapper mapper)
    {
        _myWorldDbContext = myWorldDbContext;
        _mapper = mapper;
    }
    public async Task<int> Handle(CreateBeachRequest request, CancellationToken cancellationToken)
    {
        var newBeach = _mapper.Map<Backend.Domain.Entities.Beach>(request);
        _myWorldDbContext.Beach.Add(newBeach);
        await _myWorldDbContext.SaveToDbAsync();
        return newBeach.Id;
    }
}