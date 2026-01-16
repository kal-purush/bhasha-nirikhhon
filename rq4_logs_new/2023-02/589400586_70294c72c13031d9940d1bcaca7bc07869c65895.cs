using CosmosQuery.ExpressionBuilders;
using CosmosQuery.Extensions;
using CosmosQuery.Query;
using LogicBuilder.Expressions.Utils;
using Microsoft.AspNetCore.OData.Query;
using System.Runtime.InteropServices;

namespace CosmosQuery.Visitors;

internal sealed class QueryMethodAppender : ProjectionVisitorBase
{
    private readonly PathSegment collectionSegment;

    private QueryMethodAppender(List<PathSegment> pathSegments, ODataQueryContext context)
        : base(pathSegments, context)
    {
        this.collectionSegment = this.pathSegments.Last(e => e.MemberType.IsList());
    }

    public static Expression AppendFilters(Expression expression, List<PathSegment> pathSegments, ODataQueryContext context) =>
        new QueryMethodAppender(pathSegments, context).Visit(expression);

    protected override Expression MatchedExpression(PathSegment pathSegment, MemberInitExpression node, MemberAssignment binding)
    {
        if (pathSegment != this.collectionSegment)
            return base.VisitMemberInit(node);

        return Expression.MemberInit
        (
            Expression.New(node.Type),
            node.Bindings.OfType<MemberAssignment>().Select(UpdateBinding)
        );

        MemberAssignment UpdateBinding(MemberAssignment assignment)
        {
            if (assignment != binding)
                return assignment;

            return assignment.Update(GetBindingExpression(binding));
        }
    }

    private Expression GetBindingExpression(MemberAssignment binding)
    {
        Span<PathSegment> segments = CollectionsMarshal.AsSpan(this.pathSegments);
        ref PathSegment lastSegment = ref segments[^1];

        Type elementType = this.collectionSegment.ElementType;

        Expression expression = binding.Expression.NodeType == ExpressionType.Call
            ? GetCallExpression(binding.Expression, lastSegment)
            : GetMemberAccessExpression(binding.Expression, lastSegment);

        return expression;

        Expression GetCallExpression(Expression expression, in PathSegment segment) =>
            expression.GetQueryableExpression
            (
                this.pathSegments,
                segment.QueryOptions,
                this.context
            );

        Expression GetMemberAccessExpression(Expression expression, in PathSegment segment)
        {
            Expression queryExpression = expression.GetQueryableMethod
            (
                this.context,
                segment.QueryOptions.OrderByClause,
                elementType,
                segment.QueryOptions.Skip,
                segment.QueryOptions.Top
            );

            return queryExpression.Type.IsArray
                ? queryExpression.ToArrayCall(elementType)
                : queryExpression.ToListCall(elementType);
        }

    }
}