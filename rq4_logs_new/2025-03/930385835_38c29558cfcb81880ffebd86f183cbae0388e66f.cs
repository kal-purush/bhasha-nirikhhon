using System.Text;
using Dapper;

namespace PetFamily.Application.Pets.Queries.GetFilteredPetsWithPaginationDapper;

public static class DapperExtensions
{
    public static void ApplySorting(
        this StringBuilder sqlBuilder,
        DynamicParameters parameters,
        string? sortBy,
        string? sortDirection)
    {
        var sortingParameter = sortBy?.ToLower() ?? "id";
        var sortingDirection = sortDirection?.ToUpper() ?? "DESC";
        sqlBuilder.Append($"\nORDER BY {sortingParameter} {sortingDirection}");
        //parameters.Add("@orderBy", orderBy?.ToLower() ?? "id");
        //parameters.Add("@SortDirection", sortDirection?.ToUpper() ?? "DESC");
        //sqlBuilder.Append("ORDER BY name DESC");
        //sqlBuilder.Append(" ORDER BY @sortBy ");
        //sqlBuilder.Append("@SortDirection");
    }

    public static void ApplyPagination(
        this StringBuilder sqlBuilder,
        DynamicParameters parameters,
        int page,
        int pageSize)
    {
        parameters.Add("@PageSize", pageSize);
        parameters.Add("@Offset", (page - 1) * pageSize);

        sqlBuilder.Append("\nLimit @PageSize OFFSET @Offset");
    }
}