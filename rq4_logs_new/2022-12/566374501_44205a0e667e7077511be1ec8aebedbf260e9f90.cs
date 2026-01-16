using MediatR;
using Standard.Tool.Platform.Data.Entities;
using Standard.Tool.Platform.Data.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Auth.PermissionFeature;

public record CountPermissionsQuery : IRequest<int>;

public class CountPermissionsQueryHandler : IRequestHandler<CountPermissionsQuery, int>
{
    private readonly IRepository<PermissionEntity> _repo;

    public CountPermissionsQueryHandler(IRepository<PermissionEntity> repo) => _repo = repo;

    public Task<int> Handle(CountPermissionsQuery request, CancellationToken ct) => _repo.CountAsync(ct: ct);
}