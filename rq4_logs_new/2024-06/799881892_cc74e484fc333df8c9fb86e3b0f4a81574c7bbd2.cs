using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Application.Common.Models;
using AutoMapper.QueryableExtensions;
using AutoMapper;

namespace Application.Common.Mappings
{
    /// <summary>
    /// Extensions of the <see cref="IQueryable"/> interface
    /// for mapping to <see cref="PaginatedList{T}"/> 
    /// or <see cref="List{T}"/>.
    /// </summary>
    public static class MappingExtensions
    {
        /// <summary>
        /// Maps to a <see cref="PaginatedList{T}"/> containing the items
        /// in the specified <paramref name="pageNumber"/>.
        /// </summary>
        public static Task<PaginatedList<TDestination>> PaginatedListAsync<TDestination>(this IQueryable<TDestination> queryable, int pageNumber, int pageSize) where TDestination : class
            => PaginatedList<TDestination>.CreateAsync(queryable.AsNoTracking(), pageNumber, pageSize);

        /// <summary>
        /// Maps to a <see cref="List{T}"/>.
        /// </summary>
        public static Task<List<TDestination>> ProjectToListAsync<TDestination>(this IQueryable queryable, IConfigurationProvider configuration) where TDestination : class
            => queryable.ProjectTo<TDestination>(configuration).AsNoTracking().ToListAsync();
    }
}