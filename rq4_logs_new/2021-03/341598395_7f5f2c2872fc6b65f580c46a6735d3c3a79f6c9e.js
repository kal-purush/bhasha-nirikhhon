public void ConfigureServices(IServiceCollection services)
{
    services.AddMvc();
 
    // Add swagger config
    services.AddSwaggerGen(options => {
        options.SwaggerDoc("v1",
             new Info
             {
                  Title = "Xacalli",
                  Version = "1.0",
                  Description = "About Hotel Xacalli",
                  TermsOfService = "None"
             }
         );
 
         options.DescribeAllEnumsAsStrings();
     });
}
 
public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
{
    ...
    app.UseSwagger(c =>
    {
         c.PreSerializeFilters.Add((swagger, httpReq) => swagger.Host = httpReq.Host.Value);
    });
    app.UseSwaggerUI(c =>
    {
         c.SwaggerEndpoint("swagger.json", "V1 Docs");
    });
}