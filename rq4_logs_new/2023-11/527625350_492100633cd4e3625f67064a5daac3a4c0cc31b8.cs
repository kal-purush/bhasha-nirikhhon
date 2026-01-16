
using System.Text.Json.Serialization;

namespace Platformer.Component
{
    public class OneWayPlatform
    {
        public PlatformDirection Direction { get; }

        public OneWayPlatform(PlatformDirection direction)
        {
            Direction = direction;
        }

        [JsonConverter(typeof(JsonStringEnumConverter))]
        public enum PlatformDirection
        {
            UP = 1,
            DOWN = 2,
            LEFT = 3,
            RIGHT = 4
        }
    }
}