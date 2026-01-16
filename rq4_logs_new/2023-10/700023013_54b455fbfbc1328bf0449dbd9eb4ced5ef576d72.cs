using Application.Commons;
using Application.Commons.Dtos;
using Application.Regions.Entities;
using AutoMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Application.Regions.Features.GetAllCountryCodes;

public class GetAllCountryCodesHandler : IRequestHandler<GetAllCountryCodesRequest, GetAllLibResponse>
{
    private readonly IMapper _mapper;
    private readonly AppDbContext _ctx;

    public GetAllCountryCodesHandler(AppDbContext ctx, IMapper mapper)
    {
        _ctx = ctx;
        _mapper = mapper;
    }

    public async Task<GetAllLibResponse> Handle(GetAllCountryCodesRequest request, CancellationToken cancellationToken)
    {
        var countryCodes = await _ctx.GetEntity<CountryCode>()
            .ToListAsync(cancellationToken);

        return _mapper.Map<GetAllLibResponse>(countryCodes);
    }
}