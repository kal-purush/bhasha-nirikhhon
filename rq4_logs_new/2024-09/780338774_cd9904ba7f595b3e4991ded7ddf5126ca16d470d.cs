using AutoMapper;
using MB.Application.Exceptions;
using MB.Application.Interfaces.Persistence.Common;
using MB.Domain.Entities;
using MediatR;

namespace MB.Application.Features.Videos.Queries.GetOneVideoDetails;

public class GetOneVideoDetailsQueryHandler(IMapper mapper, IBaseRepository<Video> videoRepository) : IRequestHandler<GetOneVideoDetailsQuery, GetOneVideoDetailsQueryResponse>
{
    private readonly IMapper _mapper = mapper;
    private readonly IBaseRepository<Video> _videoRepository = videoRepository;

    public async Task<GetOneVideoDetailsQueryResponse> Handle(GetOneVideoDetailsQuery request, CancellationToken cancellationToken)
    {
        var video = await _videoRepository.GetByBusinessIdAsync(request.VideoId)
            ?? throw new NotFoundException("Video not found.");

        var videoDto = _mapper.Map<GetOneVideoDetailsQueryDto>(video);

        return new GetOneVideoDetailsQueryResponse
        {
            Success = true,
            Message = "Success.",
            Video = videoDto
        };
    }
}