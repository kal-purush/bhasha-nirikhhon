namespace AzurePipelines.Toolkit;

using AzurePipelines.Toolkit.Commands;
using AzurePipelines.Toolkit.Variables;

/// <inheritdoc />
public sealed class AzurePipelinesToolkit : IAzurePipelinesToolkit
{
    private readonly Action<string> sink;

    public AzurePipelinesToolkit()
        : this(Console.WriteLine)
    {
    }

    public AzurePipelinesToolkit(Action<string> sink)
    {
        this.sink = sink;

        this.Format = new FormattingCommands(this.sink);
        this.Task = new TaskCommands(this);
        this.Artifact = new ArtifactCommands(this);
        this.Build = new BuildCommands(this);
        this.Release = new ReleaseCommands(this);
        this.Variables = new PredefinedVariables();
    }

    /// <inheritdoc />
    public IFormattingCommands Format { get; }

    /// <inheritdoc />
    public ITaskCommands Task { get; }

    /// <inheritdoc />
    public IArtifactCommands Artifact { get; }

    /// <inheritdoc />
    public IBuildCommands Build { get; }

    /// <inheritdoc />
    public IReleaseCommands Release { get; }

    /// <inheritdoc />
    public IPredefinedVariables Variables { get; }

    /// <summary>
    /// </summary>
    /// <param name="command"></param>
    /// <param name="message"></param>
    /// <param name="properties"></param>
    internal void WriteCommand(
        string command,
        string? message = null,
        IDictionary<string, object>? properties = default)
    {
        var formattedProperties =
            properties?
                .Select(x => $"{x.Key}={EscapeValue(x.Value.ToString())}")
            ?? Enumerable.Empty<string>();

        this.Write(command, formattedProperties, message);
    }

    private static string? EscapeMessage(string? data)
    {
        return data?
            .Replace("\r", "%0D")
            .Replace("\n", "%0A");
    }

    private static string? EscapeValue(string? value)
    {
        return value?
            .Replace("\r", "%0D")
            .Replace("\n", "%0A")
            .Replace("]", "%5D")
            .Replace(";", "%3B");
    }

    private void Write(string command, IEnumerable<string> formattedProperties, string? message)
    {
        this.sink.Invoke($"##vso[{command} {string.Join(';', formattedProperties)}]{EscapeMessage(message)}");
    }
}