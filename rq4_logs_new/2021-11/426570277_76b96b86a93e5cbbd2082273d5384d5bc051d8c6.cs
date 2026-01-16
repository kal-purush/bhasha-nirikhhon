using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;
using System.Web.Http.Filters;
using Profile.Application.Exceptions;
using FluentValidation.Results;

namespace Profile.API.Filters
{
    public class HeaderValidatorAttribute : ActionFilterAttribute
    {
        private readonly string _header;
        public HeaderValidatorAttribute(string header)
        {
            _header = header;
        }

        public override void OnActionExecuting(HttpActionContext actionContext)
        {
            if (!actionContext.Request.Headers.TryGetValues(_header, out _))
            {
                var errors = new List<ValidationFailure> { new ValidationFailure("", "UserId header missing") };
                throw new ValidationException(errors);
            }
            base.OnActionExecuting(actionContext);
        }

        public override Task OnActionExecutingAsync(HttpActionContext actionContext, CancellationToken cancellationToken)
        {
            if (!actionContext.Request.Headers.TryGetValues(_header, out _))
            {
                var errors = new List<ValidationFailure> { new ValidationFailure("", "UserId header missing") };
                throw new ValidationException(errors);
            }

            return base.OnActionExecutingAsync(actionContext, cancellationToken);
        }
    }
}