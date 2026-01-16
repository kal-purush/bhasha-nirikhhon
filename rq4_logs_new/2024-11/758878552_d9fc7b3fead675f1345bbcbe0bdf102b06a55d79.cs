using Jared.Shared.Abstractions;
using Jared.Shared.Dtos.Role;
using MediatR;
using System.Net.Http.Json;

namespace Jared.Presentation.Requests.Roles.List;

public class RoleListQueryHandler : IRequestHandler<RoleListQuery, Result<List<RoleListDto>>>
{
    private readonly HttpClient httpClient;

    public RoleListQueryHandler(HttpClient httpClient)
    {
        this.httpClient = httpClient;
    }

    public async Task<Result<List<RoleListDto>>> Handle(RoleListQuery request, CancellationToken cancellationToken)
    {
        string baseUrl = BaseAdresses.ROLE_LIST;

        var response = await httpClient.GetFromJsonAsync<Result<List<RoleListDto>>>(baseUrl, cancellationToken);

        if (response is null)
        {
            return Result.Fail<List<RoleListDto>>("Invalid response type");
        }

        return response;
    }
}