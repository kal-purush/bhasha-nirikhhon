using MarGate.Core.Logging.Configuration;
using Serilog.Events;
using Serilog.Formatting;

namespace MarGate.Core.Logging.LogTargets;

public class ConsoleLogSettings : ILogTargetSettings
{
    public ITextFormatter LogFormatter { get; set; } = new Serilog.Formatting.Display.MessageTemplateTextFormatter("[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}");
    public bool IncludeTimestamp { get; set; } = true;
    public LogEventLevel LogLevel { get; set; } = LogEventLevel.Information;
}