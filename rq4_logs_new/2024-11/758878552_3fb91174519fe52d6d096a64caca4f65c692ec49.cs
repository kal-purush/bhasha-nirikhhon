using Jared.Domain.Models;
using Jared.Shared.Abstractions;
using Jared.Shared.Dtos.Role;
using Jared.Shared.Interfaces;
using MapsterMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Jared.Application.Requests.Roles.List;

public class RoleListQueryHandler : IRequestHandler<RoleListQuery, Result<List<RoleListDto>>>
{
    private readonly IDataContext dataContext;
    private readonly IMapper mapper;

    public RoleListQueryHandler(IDataContext dataContext, IMapper mapper)
    {
        this.dataContext = dataContext;
        this.mapper = mapper;
    }

    public async Task<Result<List<RoleListDto>>> Handle(RoleListQuery request, CancellationToken cancellationToken)
    {
        try
        {
            var rolesQuery = dataContext
                .Set<Role>()
                .AsNoTracking();

            var roles = await rolesQuery.ToListAsync();

            var result = mapper.Map<List<RoleListDto>>(roles);

            return Result.Ok(result);
        }
        catch (Exception ex)
        {
            return Result.Fail<List<RoleListDto>>(ex.Message);
        }
    }
}