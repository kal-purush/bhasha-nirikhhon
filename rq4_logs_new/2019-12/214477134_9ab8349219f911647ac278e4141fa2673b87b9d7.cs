using CentralDeErros.Application.ViewModel;
using System.Collections.Generic;
using System.Linq;

namespace CentralDeErros.Api.GraphQL.Filters
{
    public static class OrderErrorLogByExtension
    {
        public static IList<ErrorLogViewModel> OrderErrorLogBy(this IEnumerable<ErrorLogViewModel> source, OrderByEnum.OrderBy orderBy)
        {
            switch (orderBy)
            {
                case OrderByEnum.OrderBy.Code:
                    return source.OrderBy(p => p.Code).ToList();
                case OrderByEnum.OrderBy.Level:
                    return source.OrderBy(p => p.Level).ToList();
                case OrderByEnum.OrderBy.Message:
                    return source.OrderBy(p => p.Message).ToList();
                default:
                    return source.ToList();
            }

        }
    }
}