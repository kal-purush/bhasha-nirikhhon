namespace Snailer.GodotCSharp.SettingsManager;

using System.Reflection;
using Newtonsoft.Json;

public static class JsonFileHelper
{
    /// <summary>
    /// Ensures that the JSON file exists. If not, it is created with the default <typeparamref name="T"/> object.
    /// </summary>
    public static void EnsureJsonFile<T>(string fileName)
    {
        ValidateFileName(fileName);
        if (File.Exists(GetPathToFile(fileName)))
        {
            return;
        }

        WriteJsonFile(Activator.CreateInstance<T>(), fileName);
    }

    /// <summary>
    /// Gets the full path of a file within the executing directory.
    /// </summary>
    public static string GetPathToFile(string fileName)
    {
        var executingDir = Assembly.GetExecutingAssembly().Location;
        if (executingDir.EndsWith(".dll"))
        {
            executingDir = Path.GetDirectoryName(executingDir);
        }

        if (executingDir is null)
        {
            throw new InvalidOperationException("Unable to load executing directory.");
        }

        return Path.Combine(executingDir, fileName);
    }

    /// <summary>
    /// Reads a JSON file from the filesystem and returns the resulting object.
    /// </summary>
    /// <exception cref="FileNotFoundException"></exception>
    /// <exception cref="JsonReaderException"></exception>
    public static T ReadJsonFile<T>(string fileName)
    {
        ValidateFileName(fileName);
        var path = GetPathToFile(fileName);
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"The settings file '{path}' was not found.");
        }

        var text = File.ReadAllText(path);

        return JsonConvert.DeserializeObject<T>(text)
          ?? throw new JsonReaderException($"The settings file '{path}' cannot be deserialized.");
    }

    /// <summary>
    /// Writes the provided object to the filesystem as JSON.
    /// </summary>
    public static void WriteJsonFile(object? obj, string fileName)
    {
        ValidateFileName(fileName);
        File.WriteAllText(GetPathToFile(fileName), JsonConvert.SerializeObject(obj, Formatting.Indented));
    }

    private static void ValidateFileName(string fileName)
    {
        if (!fileName.EndsWith(".json"))
        {
            throw new InvalidOperationException($"File {fileName} is not a .json file.");
        }
    }
}