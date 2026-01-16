using Ardalis.SmartEnum;

namespace Codend.Domain.Core.Enums;

/// <summary>
/// Default task status values for new projects.
/// </summary>
public sealed class DefaultTaskStatus : SmartEnum<DefaultTaskStatus>
{
    public static readonly DefaultTaskStatus ToDo = new DefaultTaskStatus(nameof(ToDo), 1);
    public static readonly DefaultTaskStatus InProgress = new DefaultTaskStatus(nameof(InProgress), 2);
    public static readonly DefaultTaskStatus Done = new DefaultTaskStatus(nameof(Done), 3);
    
    public DefaultTaskStatus(string name, int value) : base(name, value)
    {
    }
}