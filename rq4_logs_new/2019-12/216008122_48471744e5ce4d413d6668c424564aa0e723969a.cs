using AutoMapper;
using DB.Api.Application.Models;
using DB.Api.Application.Queries;
using DB.Core.Interfaces;
using MediatR;
using System.Threading;
using System.Threading.Tasks;

namespace DB.Api.Application.QueryHandlers
{
    public class GetRefreshTokenByTokenQueryHandler : IRequestHandler<GetRefreshTokenByTokenQuery, GetRefreshTokenByTokenQueryResponse>
    {
        private readonly IMapper _mapper;
        private readonly IRefreshTokenRepository _refreshTokenRepository;

        public GetRefreshTokenByTokenQueryHandler(IMapper mapper, IRefreshTokenRepository refreshTokenRepository)
        {
            _mapper = mapper;
            _refreshTokenRepository = refreshTokenRepository;
        }
        public async Task<GetRefreshTokenByTokenQueryResponse> Handle(GetRefreshTokenByTokenQuery query, CancellationToken cancellationToken)
        {
            var refreshToken = await _refreshTokenRepository.GetByTokenAsync(query.Token, cancellationToken);
            var result = _mapper.Map<GetRefreshTokenByTokenQueryResponse>(refreshToken);

            return result;
        }
    }
}