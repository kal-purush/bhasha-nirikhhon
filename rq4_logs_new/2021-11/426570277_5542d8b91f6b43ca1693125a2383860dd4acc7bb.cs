using Admin.Application.Features.Queries.SearchQuery;
using MediatR;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Admin.Application.Behaviors
{
    public class BusinessBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
    {
        public async Task<TResponse> Handle(TRequest request, CancellationToken cancellationToken, RequestHandlerDelegate<TResponse> next)
        {
            if (request is SearchProfileQuery)
            {
                var query = request as SearchProfileQuery;
                if (!query.EmpId.ToUpper().StartsWith("CTS"))
                {
                    query.EmpId = "CTS" + query.EmpId;
                }
            }
            return await next();
        }
    }
}