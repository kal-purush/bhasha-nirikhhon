using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Formatters;
using System.Text;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace AzurePipelinesToGitHubActionsConverter.Api;

public static class YamlFormatterExtensions
{
    public static IMvcBuilder AddYamlFormatters(this IMvcBuilder builder)
    {
        var serializer = new SerializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .Build();

        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .Build();

        return builder.AddMvcOptions(options =>
        {
            options.InputFormatters.Add(new YamlRawInputFormatter()); // Changed to raw formatter
            options.OutputFormatters.Add(new YamlOutputFormatter(serializer));
            options.FormatterMappings.SetMediaTypeMappingForFormat("yaml", "text/yaml");
            options.FormatterMappings.SetMediaTypeMappingForFormat("yaml", "application/x-yaml");
        });
    }
}

// NEW: Simple raw YAML input formatter
public class YamlRawInputFormatter : TextInputFormatter
{
    public YamlRawInputFormatter()
    {
        SupportedMediaTypes.Add("application/x-yaml");
        SupportedMediaTypes.Add("text/yaml");
        SupportedEncodings.Add(Encoding.UTF8);
        SupportedEncodings.Add(Encoding.Unicode);
    }

    public override async Task<InputFormatterResult> ReadRequestBodyAsync(
        InputFormatterContext context, Encoding encoding)
    {
        using var reader = new StreamReader(context.HttpContext.Request.Body, encoding);
        var content = await reader.ReadToEndAsync();
        return await InputFormatterResult.SuccessAsync(content); // Returns raw string
    }
}

// Output formatter remains the same
public class YamlOutputFormatter : TextOutputFormatter
{
    private readonly ISerializer _serializer;

    public YamlOutputFormatter(ISerializer serializer)
    {
        _serializer = serializer;
        SupportedMediaTypes.Add("text/yaml");
        SupportedMediaTypes.Add("application/x-yaml");
        SupportedEncodings.Add(Encoding.UTF8);
        SupportedEncodings.Add(Encoding.Unicode);
    }

    public override Task WriteResponseBodyAsync(
        OutputFormatterWriteContext context, Encoding selectedEncoding)
    {
        var response = context.HttpContext.Response;
        var yaml = _serializer.Serialize(context.Object);
        return response.WriteAsync(yaml);
    }
}