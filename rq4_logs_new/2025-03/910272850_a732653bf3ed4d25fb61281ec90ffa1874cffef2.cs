using System.Diagnostics;
using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Expenses.Api.Resources;
using Microsoft.Extensions.Localization;

namespace Expenses.Api.Adapters;

public class CsvHelperAdapter
{

    private readonly IServiceProvider _serviceProvider;
    public CsvHelperAdapter(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public Stream GenerateFile<T>(Stream stream, List<T> content, CultureInfo culture = null)
    {
        using var writer = new StreamWriter(stream, new UTF8Encoding(true), leaveOpen: true);
        using (var csv = new CsvWriter(writer, culture ?? CultureInfo.InvariantCulture))
        {
            csv.WriteRecords(content);

            writer.Flush();
            stream.Position = 0;
        }

        return stream;
    }

    public Stream GenerateFile<T, TMap>(Stream stream, List<T> content, CultureInfo culture = null)
        where TMap : ClassMap<T>
    {
        using var writer = new StreamWriter(stream, new UTF8Encoding(true), leaveOpen: true);
        using (var csv = new CsvWriter(writer, culture ?? CultureInfo.InvariantCulture))
        {
            csv.Context.AutoMap<TMap>();
            var classMap = CreateClassMapInstance<TMap>();
            csv.Context.RegisterClassMap(classMap);
            csv.WriteRecords(content);

            writer.Flush();
            stream.Position = 0;
        }

        return stream;
    }

    private TMap CreateClassMapInstance<TMap>() where TMap : ClassMap
    {
        var constructor = typeof(TMap).GetConstructor(new[] { typeof(IStringLocalizer<SharedResources>) });

        if (constructor != null)
        {
            var localizer = (IStringLocalizer<SharedResources>)_serviceProvider.GetService(typeof(IStringLocalizer<SharedResources>));
            return (TMap)constructor.Invoke(new object[] { localizer });
        }

        return Activator.CreateInstance<TMap>();
    }
}