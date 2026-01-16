using AutoMapper.OData.Cosmos.Tests.Models;
using Microsoft.AspNetCore.OData.Query.Expressions;
using Microsoft.OData.UriParser;
using System.Linq.Expressions;

namespace AutoMapper.OData.Cosmos.Tests.Binder;

public sealed class ForestSearchBinder : QueryBinder, ISearchBinder
{
    public Expression BindSearch(SearchClause searchClause, QueryBinderContext context)
    {
        SearchTermNode node = (SearchTermNode)searchClause.Expression;
        Expression<Func<ForestModel, bool>> exp = p => p.ForestName == node.Text;
        return exp;
    }
}
