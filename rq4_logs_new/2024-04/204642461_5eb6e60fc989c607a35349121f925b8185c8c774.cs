using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serko.Expense.Server.Formatters;

namespace Serko.Expense.Server.Options;

public class ConfigureFormatterOptions : IConfigureOptions<MvcOptions>
{
    private readonly ILoggerFactory factory;

    public ConfigureFormatterOptions(ILoggerFactory factory)
    {
        this.factory = factory;
    }

    public void Configure(MvcOptions options)
    {
        var logger = factory.CreateLogger<EmailInputFormatter>();
        var formatter = new EmailInputFormatter(logger);
        options.InputFormatters.Insert(0, formatter);
    }
}