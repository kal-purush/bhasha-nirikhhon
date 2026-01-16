using Application.Commons;
using Application.Commons.Dtos;
using Application.Users.Entities;
using AutoMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Application.Users.Features.GetAllTypes;

public class GetAllTypesHandler : IRequestHandler<GetAllTypesRequest, GetAllLibResponse>
{
    private readonly IMapper _mapper;
    private readonly AppDbContext _ctx;

    public GetAllTypesHandler(AppDbContext ctx, IMapper mapper)
    {
        _ctx = ctx;
        _mapper = mapper;
    }

    public async Task<GetAllLibResponse> Handle(GetAllTypesRequest request, CancellationToken cancellationToken)
    {
        var types = await _ctx.GetEntity<UserType>()
            .ToListAsync(cancellationToken);

        return _mapper.Map<GetAllLibResponse>(types);
    }
}