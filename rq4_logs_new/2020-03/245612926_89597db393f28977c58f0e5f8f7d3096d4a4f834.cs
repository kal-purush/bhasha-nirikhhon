using System;
using System.Collections.Generic;

namespace CourseGenerator.DAL.Pagination
{
    public class PagedList<T> : IPagedList
    {
        public int TotalPages { get; set; }
        public int TotalCount { get; set; }
        public int PageIndex { get; set; }
        public int PageSize { get; set; }
        public bool HavePreviousPage { get; set; }
        public bool HaveNextPage { get; set; }
        public IEnumerable<T> Items { get; set; }


        public PagedList(IEnumerable<T> items, int totalCount, int pageSize, int pageIndex)
        {
            PageIndex = pageIndex;
            PageSize = pageSize;

            TotalCount = totalCount;
            TotalPages = Convert.ToInt32(Math.Ceiling((double)TotalCount / (double)PageSize));

            HavePreviousPage = PageIndex > 1;
            HaveNextPage = PageIndex < TotalPages;

            Items = items;
        }
    }
}