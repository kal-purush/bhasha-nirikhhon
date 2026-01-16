using System.Text.Json.Serialization;
using NodaTime;

namespace Mortein.Types;


public enum CommandType { }


/// <summary>
/// Command for a device.
/// </summary>
public abstract class Command
{
    /// <summary>
    /// The unique identifier of a device.
    /// </summary>
    public required Guid DeviceId { get; set; }

    /// <summary>
    /// The timestamp for this command.
    /// </summary>
    public Instant Timestamp { get; set; } = SystemClock.Instance.GetCurrentInstant();

    /// <summary>
    /// The type of command.
    /// </summary>
    public abstract CommandType Type { get; }
}