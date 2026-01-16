// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");
var outputDir = "../../../../cslox/Parser";
var types = new Dictionary<string, string[]>
{
    ["Binary"] = new[] { "Expression Left", "Token Operator", "Expression Right" },
    ["Grouping"] = new[] { "Expression Expression" },
    ["Literal"] = new[] { "object Value" },
    ["Unary"] = new[] { "Token Operator", "Expression Right" },
};
DefineAst(outputDir, "Expression", types);

static void DefineAst(string outputDir, string baseName, Dictionary<string, string[]> types)
{
    var path = $"{outputDir}/{baseName}.cs";
    FileStream stream = new FileStream(path, FileMode.Create);
    using var writer = new StreamWriter(stream);
    
    writer.WriteLine("using cslox.Scanning;");
    writer.WriteLine("");
    writer.WriteLine("namespace cslox.Parser;");
    writer.WriteLine("");

    writer.WriteLine($"abstract class {baseName}");
    writer.WriteLine("{");
    writer.WriteLine("}");

    foreach (var (className, fields) in types)
    {
        DefineType(writer, baseName, className, fields);
    }
}

static void DefineType(StreamWriter writer, string baseName, string className, string[] fields)
{
    var fieldList = "";
    var fieldNames = new List<string>();
    foreach (var field in fields)
    {
        var typeAndName = field.Split(" ");
        fieldNames.Add(typeAndName[1]);

        var parameterName = typeAndName[1].ToLower();
        if (parameterName == "operator")
        {
            parameterName = $"@{parameterName}";
        }
        if (fieldList != "")
        {
            fieldList += ", ";
        }
        fieldList += $"{typeAndName[0]} {parameterName}";
    }

    writer.WriteLine("");
    writer.WriteLine($"class {className} : {baseName}");
    writer.WriteLine("{");
    writer.WriteLine($"\tpublic {className}({fieldList})");
    writer.WriteLine("\t{");
    foreach (var fieldName in fieldNames)
    {
        var parameterName = fieldName.ToLower();
        if (parameterName == "operator")
        {
            parameterName = $"@{parameterName}";
        }

        writer.WriteLine($"\t\t{fieldName} = {parameterName};");
    }
    writer.WriteLine("\t}");
    writer.WriteLine("");
    foreach (var field in fields)
    {
        writer.WriteLine($"\tpublic {field} {{ get; }}");
    }
    writer.WriteLine("}");
}