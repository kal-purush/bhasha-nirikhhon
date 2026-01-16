using System.Globalization;

using Model;
using CsvHelper;
using CsvHelper.Configuration;

namespace Utils;

public static class CSVHandler
{
    private readonly static CsvConfiguration config = new(CultureInfo.InvariantCulture)
    {
        IgnoreBlankLines = true,
    };

    public static List<Customer> Read(string filepath)
    {
        List<Customer> customers = new();

        using (StreamReader streamReader = new(filepath))
        using (CsvReader csvReader = new(streamReader, config))
        {
            customers = csvReader.GetRecords<Customer>().ToList();
        }

        return customers;
    }
}