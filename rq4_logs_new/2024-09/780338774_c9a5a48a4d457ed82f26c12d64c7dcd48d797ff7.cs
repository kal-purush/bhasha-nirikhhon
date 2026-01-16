using AutoMapper;
using MB.Application.Contracts.Persistence.Common;
using MB.Domain.Entities;
using MediatR;

namespace MB.Application.Features.Artists.Queries.GetAllArtists;

public class GetAllArtistsQueryHandler(IBaseRepository<Artist> artistRepository, IMapper mapper) : IRequestHandler<GetAllArtistsQuery, GetAllArtistsQueryResponse>
{
    private readonly IBaseRepository<Artist> _artistRepository = artistRepository;
    private readonly IMapper _mapper = mapper;

    public async Task<GetAllArtistsQueryResponse> Handle(GetAllArtistsQuery request, CancellationToken cancellationToken)
    {
        var artists = await _artistRepository.GetAllAsync(artist => artist.Name);
        var response = new GetAllArtistsQueryResponse
        {
            Success = true,
            Message = "Success.",
            Artists = _mapper.Map<List<GetAllArtistsQueryDto>>(artists)
        };

        return response;
    }
}