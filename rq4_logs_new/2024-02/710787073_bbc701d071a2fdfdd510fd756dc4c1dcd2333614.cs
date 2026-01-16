using Ncea.Harvester.Constants;

namespace Ncea.Harvester.Models;

public class HarvesterConfiguration
{
    public ProcessorType ProcessorType { get; set; }
    public string Type { get; set; } = null!;
    public string DataSourceApiBase { get; set; } = null!;
    public string DataSourceApiUrl { get; set; } = null!;
}
