using System.Linq.Expressions;
using api.Data;
using api.Shared;
using api.Users.Dtos;
using api.Users.Models;
using MediatR;

namespace api.Cryptos.Queries;
public class ListUsersQueryCommand : IRequest<PageList<UserDto>>
{
    public string? FirstName { get; set; } = string.Empty;
    public string? Email { get; set; } = string.Empty;
    public string? SortColumn { get; set; } = string.Empty;
    public string? SortOrder { get; set; } = "ASC";
    public int Page { get; set; } = 1;
    public int PageSize { get; set; }
}
public class ListUsersQueryCommandHandler : IRequestHandler<ListUsersQueryCommand, PageList<UserDto>>
{
    private readonly DataContext _context;

    public ListUsersQueryCommandHandler(DataContext context)
    {
        _context = context;
    }

    public async Task<PageList<UserDto>> Handle(ListUsersQueryCommand request, CancellationToken cancellationToken)
    {
        IQueryable<User> userQuery = _context.Users;

        int maxPageSize = 20;

        if (request.PageSize > maxPageSize)
        {
            request.PageSize = maxPageSize;
        }

        if (!string.IsNullOrEmpty(request.FirstName))
        {
            request.FirstName = request.FirstName?.ToLower().Trim();
            userQuery = userQuery.Where(x => x.FirstName.ToLower().Contains(request.FirstName));
        }

        if (!string.IsNullOrEmpty(request.Email))
        {
            request.Email = request.Email?.ToLower().Trim();
            userQuery = userQuery.Where(x => x.Email.ToLower().Contains(request.Email));
        }

        if (request.SortOrder?.ToUpper() == "DESC")
        {
            userQuery = userQuery.OrderByDescending(GetSortProperty(request));
        }
        else
        {
            userQuery = userQuery.OrderBy(GetSortProperty(request));
        }

        var collection = userQuery.Select(x => new UserDto(x.Id,
                                                           x.FirstName,
                                                           x.LastName,
                                                           x.Email,
                                                           x.Role));

        var pagedCollection = await PageList<UserDto>.CreateAsync(collection,
                                                                  request.Page,
                                                                  request.PageSize);
        return pagedCollection;

    }

    private static Expression<Func<User, object>> GetSortProperty(ListUsersQueryCommand request)
    {
        return request.SortColumn?.ToLower() switch
        {
            "email" => user => user.Email,
            "first_name" => user => user.FirstName,
            "role" => user => user.Role,
            _ => user => user.Id
        };
    }
}