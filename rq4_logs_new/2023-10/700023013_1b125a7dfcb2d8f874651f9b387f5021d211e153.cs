using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;

namespace Application.Commons.DataAccess;

public class BaseSpecification<T> : ISpecification<T>
{
    public BaseSpecification()
    {
    }

    public BaseSpecification(Expression<Func<T, bool>> criteria)
    {
        Criteria = criteria;
    }

    public Expression<Func<T, bool>> Criteria { get; }
    public List<Func<IQueryable<T>, IIncludableQueryable<T, object>>> Includes { get; } = new();
    public Expression<Func<T, object>> OrderBy { get; private set; }
    public Expression<Func<T, object>> OrderByDesc { get; private set; }
    public int Skip { get; private set; }
    public int Take { get; private set; }
    public bool IsPagingEnabled { get; private set; }

    protected void AddInclude(Func<IQueryable<T>, IIncludableQueryable<T, object>> func)
    {
        Includes.Add(func);
    }

    protected void AddOrderBy(Expression<Func<T, object>> orderByExpression)
    {
        OrderBy = orderByExpression;
    }

    protected void AddOrderByDesc(Expression<Func<T, object>> orderByDescExpression)
    {
        OrderByDesc = orderByDescExpression;
    }

    protected void ApplyPaging(int skip, int take)
    {
        Skip = skip;
        Take = take;
        IsPagingEnabled = true;
    }

    protected void ApplySorting(string fieldName, string direction)
    {
        if (!string.IsNullOrEmpty(direction))
        {
            const string descendingSuffix = "desc";

            var descending = direction.EndsWith(descendingSuffix, StringComparison.OrdinalIgnoreCase);
            var propertyName = fieldName.Substring(0, 1).ToUpperInvariant() +
                               fieldName.Substring(1, fieldName.Length - 1);

            var specificationType = GetType().BaseType;
            var targetType = specificationType.GenericTypeArguments[0];
            var property = targetType.GetRuntimeProperty(propertyName) ??
                           throw new InvalidOperationException(
                               $"Because the property {propertyName} does not exist it cannot be sorted.");

            var lambdaParamX = Expression.Parameter(targetType, "x");

            var propertyReturningExpression = Expression.Lambda(
                Expression.Convert(
                    Expression.Property(lambdaParamX, property),
                    typeof(object)),
                lambdaParamX);

            if (descending)
            {
                specificationType.GetMethod(
                        nameof(AddOrderByDesc),
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
                    .Invoke(this, new object[] { propertyReturningExpression });
            }
            else
            {
                specificationType.GetMethod(
                        nameof(AddOrderBy),
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
                    .Invoke(this, new object[] { propertyReturningExpression });
            }
        }
    }
}