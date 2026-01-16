using Application.Commons;
using Application.Commons.Dtos;
using Application.Regions.Entities;
using AutoMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Application.Regions.Features.GetAllCitizenships;

public class GetAllCitizenshipsHandler : IRequestHandler<GetAllCitizenshipsRequest, GetAllLibResponse>
{
    private readonly IMapper _mapper;
    private readonly AppDbContext _ctx;

    public GetAllCitizenshipsHandler(AppDbContext ctx, IMapper mapper)
    {
        _ctx = ctx;
        _mapper = mapper;

    }

    public async Task<GetAllLibResponse> Handle(GetAllCitizenshipsRequest request, CancellationToken cancellationToken)
    {
        var citizenships = await _ctx.GetEntity<Citizenship>()
            .ToListAsync(cancellationToken);

        return _mapper.Map<GetAllLibResponse>(citizenships);
    }
}