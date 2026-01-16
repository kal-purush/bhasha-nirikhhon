using System.Dynamic;
using System.Text.Json;

namespace DynamicSerializer;

public class CsvParser
{
    public static string GetJsonString()
    {
        var lines = File.ReadAllLines(FilePath("cities.csv"));
        var propertyNames = lines[0].Split(",");

        var dataObjects = new List<object>();

        for (var i = 1; i < lines.Length; i++)
        {
            var currentLine = lines[i].Split(",");

            //var dataObject = new ExpandoObject() as IDictionary<string, object>;
            dynamic dataObject = new Dynamic();

            for (var j = 0; j < propertyNames.Length; j++)
            {
                dataObject.AddProperty(propertyNames[j], currentLine[j]);
            }

            dataObjects.Add(dataObject);
        }

        var jsonString = JsonSerializer
            .Serialize(dataObjects,
                new JsonSerializerOptions()
                {
                    WriteIndented = true,
                    Converters =
                    {
                        new DynamicConverter()
                    }
                });

        return jsonString;
    }


    private static string FilePath(string fileName)
    {
        return Directory.GetParent(AppDomain
                .CurrentDomain.BaseDirectory)?
            .Parent?.Parent?.Parent?.FullName + "\\" + fileName;
    }
}