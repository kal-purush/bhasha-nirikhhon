using System.Diagnostics;

namespace ProcessRunner;

public record CommandBuilderOptions
{
    public CommandBuilderOptions(ShellType shellType, string file, string args)
    {
        Shell = shellType;
        File = file;
        Args = args;
    }

    public ShellType Shell { get; init; }
    public string File { get; init; }
    public string Args { get; init; }
    public string? WorkingDirectory { get; init; }
    public bool RedirectStandardOutput { get; init; }
    public bool RedirectStandardError { get; init; }
    public bool RedirectStandardInput { get; init; }

}