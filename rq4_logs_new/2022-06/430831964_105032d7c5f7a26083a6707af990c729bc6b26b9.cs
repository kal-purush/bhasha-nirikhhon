using System.Net;
using AutoMapper;
using Infrastructure.Models.Marketplaces;
using Infrastructure.Models.Marketplaces.SberMegaMarket;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Infrastructure.MediatR.Marketplaces.SberMegaMarket.Queries;

public class GetSberMegaMarketByIdRequest : IRequest<SberMegaMarketDto>
{
    public int Id { get; set; }
}

public class GetSberMegaMarketByIdHandler : IRequestHandler<GetSberMegaMarketByIdRequest, SberMegaMarketDto>
{
    private readonly MContext _context;
    private readonly IMapper _mapper;

    public GetSberMegaMarketByIdHandler(MContext context, IMapper mapper)
    {
        _context = context;
        _mapper = mapper;
    }
    
    public async Task<SberMegaMarketDto> Handle(GetSberMegaMarketByIdRequest request, CancellationToken cancellationToken)
    {
        SberMegaMarketDto? result = await _context.Marketplaces
            .AsNoTracking()
            .Where(m => m.Id == request.Id && m.Type == MarketplaceType.SberMegaMarket)
            .Select(m => _mapper.Map<SberMegaMarketDto>(m))
            .SingleOrDefaultAsync(cancellationToken);

        if (result is not null) return result;
        
        var message = $"SberMegaMarket with {request.Id} id not found";
        throw new HttpRequestException(message, null, HttpStatusCode.NotFound);
    }
}