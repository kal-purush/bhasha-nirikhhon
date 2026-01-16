using System;
using System.Linq;
using System.Threading.Tasks;
using BookLib.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace BookLib.Filter
{
    //检查是否存在author的filter
    public class CheckAuthorExistFilterAttribute: ActionFilterAttribute
    {
        public IRepositoryWrapper RepositoryWrapper { get; }
        public CheckAuthorExistFilterAttribute(IRepositoryWrapper repositoryWrapper)
        {
            RepositoryWrapper = repositoryWrapper;
        }

        public override async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
        {
            //获取action传入的参数，此时是获取参数authorId的值
            var authorIdParameter = context.ActionArguments.Single(m => m.Key == "authorId");
            Guid authorId = (Guid)authorIdParameter.Value;

            var isExist = await RepositoryWrapper.Author.IsExistAsync(authorId);
            if (!isExist)
            {
                Console.WriteLine("This authorId was not found!");
                context.Result = new NotFoundResult();
            }

            await base.OnActionExecutionAsync(context, next);
        }
    }
}