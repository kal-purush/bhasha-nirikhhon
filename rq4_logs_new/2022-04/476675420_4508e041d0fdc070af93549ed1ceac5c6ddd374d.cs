
public static class ApplicationBuilderExtension
{
    public static IApplicationBuilder UseAngularLocalDevelopmentServer(this IApplicationBuilder app, string angularProject, string angularUrl = "http://localhost:4200", string swaggerUrl = "/swagger/index.html")
    {
        app.Use(async (context, next) =>
        {
            try
            {
                await next();
            }
            catch (System.Net.Http.HttpRequestException)
            {
                if (!string.IsNullOrEmpty(swaggerUrl?.Trim()))
                {
                    context.Response.Redirect(swaggerUrl);
                    return;
                }

                throw;

            }
        }).UseSpa(spa =>
        {
            spa.Options.SourcePath = angularProject;
            spa.UseProxyToSpaDevelopmentServer(angularUrl);
        });
        return app;
    }
}