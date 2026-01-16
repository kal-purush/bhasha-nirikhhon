namespace Blacktau.OpenAuth.AspNet.Authorization.Facebook
{
    using System;

    using Blacktau.OpenAuth.AspNet.Authorization.Interfaces;

    using Microsoft.AspNetCore.Builder;
    using Microsoft.Extensions.Options;

    public static class FacebookAuthorizationAppBuilderExtensions
    {
        public static IApplicationBuilder UseFacebookAuthorization(this IApplicationBuilder app, FacebookAuthorizationOptions options)
        {
            if (app == null)
            {
                throw new ArgumentNullException(nameof(app));
            }

            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            IOpenAuthorizationResourceProviderRegistery registery = app.ApplicationServices.GetService(typeof(IOpenAuthorizationResourceProviderRegistery)) as IOpenAuthorizationResourceProviderRegistery;

            registery?.Register(options);

            return app.UseMiddleware<OpenAuthorizationMiddleware<FacebookAuthorizationOptions>>(Options.Create(options));
        }
    }
}