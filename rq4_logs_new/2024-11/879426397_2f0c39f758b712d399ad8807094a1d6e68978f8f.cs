using Newtonsoft.Json.Linq;
using System;
using System.IO;

public static class OcelotConfigHelper
{
    public static string GenerateOcelotConfig()
    {
        var ocelotJsonPath = "ocelot.json";
        var ocelotConfig = JObject.Parse(File.ReadAllText(ocelotJsonPath));

        var peopleMsHost = Environment.GetEnvironmentVariable("PEOPLE_MS_HOST") ?? "localhost";
        var peopleMsPort = int.TryParse(Environment.GetEnvironmentVariable("PEOPLE_MS_PORT"), out var peoplePort) ? peoplePort : 5010;

        var placeMsHost = Environment.GetEnvironmentVariable("PLACE_MS_HOST") ?? "localhost";
        var placeMsPort = int.TryParse(Environment.GetEnvironmentVariable("PLACE_MS_PORT"), out var placePort) ? placePort : 5020;

        var docsMsHost = Environment.GetEnvironmentVariable("DOCS_MS_HOST") ?? "localhost";
        var docsMsPort = int.TryParse(Environment.GetEnvironmentVariable("DOCS_MS_PORT"), out var docsPort) ? docsPort : 5030;

        var routesArray = ocelotConfig["Routes"] as JArray;
        if (routesArray != null)
        {
            SetRouteConfig(routesArray, "/people-ms", peopleMsHost, peopleMsPort);
            SetRouteConfig(routesArray, "/place-ms", placeMsHost, placeMsPort);
            SetRouteConfig(routesArray, "/docs-ms", docsMsHost, docsMsPort);
        }

        var tempConfigPath = "ocelot_temp.json";
        File.WriteAllText(tempConfigPath, ocelotConfig.ToString());

        return tempConfigPath;
    }

    private static void SetRouteConfig(JArray routesArray, string pathTemplate, string host, int port)
    {
        var route = routesArray.FirstOrDefault(r => r["UpstreamPathTemplate"]?.ToString().Contains(pathTemplate) == true);
        if (route != null)
        {
            var downstreamHostAndPorts = route["DownstreamHostAndPorts"] as JArray;
            if (downstreamHostAndPorts != null && downstreamHostAndPorts.Count > 0)
            {
                downstreamHostAndPorts[0]["Host"] = host;
                downstreamHostAndPorts[0]["Port"] = port;
            }
        }
    }
}