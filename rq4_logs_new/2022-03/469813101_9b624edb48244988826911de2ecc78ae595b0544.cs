using System;
using System.IO;
using Ardalis.GuardClauses;
using Newtonsoft.Json.Linq;

namespace RemotePowerSupplyGui.Config;

public class ConfigReader
{
    public static T LoadConfig<T>(string configPath) where T : IConfig
    {
        Guard.Against.InvalidPath(configPath,nameof(configPath));
        try
        {
            
            var json = JObject.Parse(File.ReadAllText(configPath));
            return json.ToObject<T>() ?? throw new InvalidOperationException();
        }
        catch(Exception e) when (e is InvalidOperationException)
        {
            
        }

        throw new InvalidOperationException();
    }
}

public interface IConfig
{
}