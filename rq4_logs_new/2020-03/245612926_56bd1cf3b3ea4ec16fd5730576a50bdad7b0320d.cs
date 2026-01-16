using AutoMapper;
using CourseGenerator.DAL.Pagination;
using System;
using System.Collections.Generic;
using System.Text;

namespace CourseGenerator.BLL.Extensions
{
    public static class PagedListExtensions
    {
        public static PagedList<TDest> ConvertPagedList<TSrc, TDest>(this PagedList<TSrc> src, IMapper mapper)
        {
            IEnumerable<TDest> destItems = mapper.Map<IEnumerable<TDest>>(src.Items);
            return new PagedList<TDest>(destItems, src.TotalCount, src.PageSize, src.PageIndex);
        }
    }
}