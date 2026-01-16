using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace IdentityServer4.Quickstart.UI
{
    public class SecurityHeadersAttribute : ActionFilterAttribute
    {
        public override void OnResultExecuting(ResultExecutingContext context)
        {
            if (!(context.Result is ViewResult))
                return;

            ensureContentTypeOptionsHeader(context);
            ensureFrameOptionsHeader(context);
            ensureContentSecurityPolicyHeader(context);
            ensureReferrerPolicyHeader(context);
        }

        /// <summary>
        /// For more information <see cref="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Referrer-Policy"/>
        /// </summary>
        /// <param name="context">Http pipeline context</param>
        private void ensureReferrerPolicyHeader(ResultExecutingContext context)
        {
            var referrer_policy = "no-referrer";
            if (!context.HttpContext.Response.Headers.ContainsKey("Referrer-Policy"))
                context.HttpContext.Response.Headers.Add("Referrer-Policy", referrer_policy);
        }

        /// <summary>
        /// For more information <see cref="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy"/>
        /// </summary>
        /// <param name="context">Http pipeline context</param>
        private void ensureContentSecurityPolicyHeader(ResultExecutingContext context)
        {
            // add upgrade-insecure-requests once you have HTTPS in place for production
            var csp = "default-src 'self'; "
                      + "object-src 'none'; "
                + "frame-ancestors 'none'; "
                + "sandbox allow-forms allow-same-origin allow-scripts; "
                + "base-uri 'self';";
            // + "upgrade-insecure-requests;";

            // standards compliant browsers
            if (!context.HttpContext.Response.Headers.ContainsKey("Content-Security-Policy"))
                context.HttpContext.Response.Headers.Add("Content-Security-Policy", csp);
            // IE
            if (!context.HttpContext.Response.Headers.ContainsKey("X-Content-Security-Policy"))
                context.HttpContext.Response.Headers.Add("X-Content-Security-Policy", csp);
        }

        /// <summary>
        /// For more information <see cref="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Frame-Options"/>
        /// </summary>
        /// <param name="context">Http pipeline context</param>
        private void ensureFrameOptionsHeader(ResultExecutingContext context)
        {
            if (!context.HttpContext.Response.Headers.ContainsKey("X-Frame-Options"))
                context.HttpContext.Response.Headers.Add("X-Frame-Options", "SAMEORIGIN");
        }

        /// <summary>
        /// For more information <see cref="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Content-Type-Options"/>
        /// </summary>
        /// <param name="context">Http pipeline context</param>
        private void ensureContentTypeOptionsHeader(ResultExecutingContext context)
        {
            if (!context.HttpContext.Response.Headers.ContainsKey("X-Content-Type-Options"))
                context.HttpContext.Response.Headers.Add("X-Content-Type-Options", "nosniff");
        }
    }
}