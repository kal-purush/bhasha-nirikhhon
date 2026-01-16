using AutoMapper;

namespace Event_Planning_System.Helpers
{
    public class CustomPaginatedListMapper<TSource, TDestination> : ITypeConverter<PaginatedList<TSource>, PaginatedList<TDestination>>
    {
        public PaginatedList<TDestination> Convert(PaginatedList<TSource> source, PaginatedList<TDestination> destination, ResolutionContext context)
        {
            // Map the list of items
            var items = context.Mapper.Map<List<TDestination>>(source.ToList());

            // Create a new instance of PaginatedList<TDestination> with the mapped items and source properties
            return new PaginatedList<TDestination>(items, source.TotalCount, source.CurrentPage, source.PageSize);
        }
    }

}