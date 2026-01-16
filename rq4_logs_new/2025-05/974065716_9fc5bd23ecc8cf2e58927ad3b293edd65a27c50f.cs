using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using static EasySave.Model.IConfigurationManager;


public interface IConfigurationFile
{
    void Save(JsonArray configuration) ;
    JsonArray Read(string path);
}

public class ConfigurationFile : IConfigurationFile
{
    public void Save(JsonArray configuration)
    {
        // Implementation for saving the configuration
        Console.WriteLine("Configuration saved.");
    }
    public JsonArray Read(string path)
    {
        // Implementation for reading the configuration with a lock
        // This is a placeholder implementation
        Console.WriteLine($"Reading configuration from {path}");
        return new JsonArray();
    }
}