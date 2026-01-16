using AutoMapper;
using Core.BaseModels;
using Core.Head.Wrappers;
using Microsoft.AspNetCore.OData.Query;
using Microsoft.EntityFrameworkCore;
using System.Linq.Expressions;

namespace Core.Head.Helpers
{
    public static class QueryHelpers
    {
        public static async Task<PageResponse<TResponse>> GetPageDataAsync<TEntity, TResponse>(
            IQueryable<TEntity> query,
            ODataQueryOptions<TEntity> oData,
            IMapper mapper,
            Func<string, Expression<Func<TEntity, bool>>>? buildSearch = null,
            CancellationToken cancellationToken = default)
            where TEntity : class, IEntity
            where TResponse : class
        {

            // Apply includes
            if (!string.IsNullOrEmpty(oData.SelectExpand?.RawExpand))
            {
                var includes = oData.SelectExpand.RawExpand.Split(',');

                foreach (var include in includes)
                {
                    query = query.Include(include);
                }
            }

            // Apply filter
            if (oData.Filter != null)
            {
                query = (IQueryable<TEntity>)oData.Filter.ApplyTo(query, new ODataQuerySettings());
            }

            // Apply search
            if (buildSearch is not null && !string.IsNullOrEmpty(oData.Search?.RawValue))
            {
                query = query.Where(buildSearch(oData.Search.RawValue));
            }

            var totalRecords = await query.CountAsync(cancellationToken).ConfigureAwait(false);

            // Apply order by
            if (oData.OrderBy != null)
            {
                query = oData.OrderBy.ApplyTo(query);
            }

            // Apply skip
            if (oData.Skip != null)
            {
                query = oData.Skip.ApplyTo(query, new ODataQuerySettings());
            }

            // Apply top
            if (oData.Top != null)
            {
                query = oData.Top.ApplyTo(query, new ODataQuerySettings());
            }

            int pageSize = oData.Top?.Value ?? totalRecords;
            int pageNumber = oData.Skip?.Value / pageSize + 1 ?? 1;

            var entities = await query.ToListAsync(cancellationToken).ConfigureAwait(false);

            var response = mapper.Map<List<TResponse>>(entities);

            return PageResponse<TResponse>.Ok(
                response,
                pageNumber,
                pageSize,
                totalRecords);
        }
    }
}