using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Filters;

namespace F1.Presentation.Filters.SetStateBag;

public sealed class F1SetStateBagFilter : IAsyncActionFilter
{
    private readonly IHttpContextAccessor _httpContextAccessor;

    public F1SetStateBagFilter(IHttpContextAccessor httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor;
    }

    public async Task OnActionExecutionAsync(
        ActionExecutingContext context,
        ActionExecutionDelegate next
    )
    {
        var stateBag = new F1StateBag();

        _httpContextAccessor.HttpContext.Items.Add(nameof(F1StateBag), stateBag);

        await next();
    }
}