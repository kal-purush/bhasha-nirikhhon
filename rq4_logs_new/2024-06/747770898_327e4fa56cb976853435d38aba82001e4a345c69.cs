using System.ComponentModel.DataAnnotations;

namespace NLogFlake.Models.Options;

public sealed class LogFlakeSettingsOptions
{
    public const string SectionName = "LogFlakeSettings";

    [Required]
    public bool AutoLogRequest { get; set; }

    [Required]
    public bool AutoLogGlobalExceptions { get; set; }

    [Required]
    public bool AutoLogUnauthorized { get; set; }

    [Required]
    public bool AutoLogResponse { get; set; }

    [Required]
    public bool PerformanceMonitor { get; set; }

    public IEnumerable<string>? ExcludedPaths { get; set; }
}