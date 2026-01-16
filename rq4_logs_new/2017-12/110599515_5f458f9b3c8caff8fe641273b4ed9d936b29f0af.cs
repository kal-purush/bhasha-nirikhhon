namespace PetProjects.Framework.Kafka.Contracts.Utils
{
    using System;

    /// <summary>
    /// Timestamp class to allow the usage of an Epoch timestamp in the contracts.
    /// </summary>
    public class Timestamp
    {
        public static readonly DateTime UnixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Creates a timestamp based in DateTime used as an argument in the ctor minus the UnixTimeEpoch (1970, 1, 1)
        /// </summary>
        /// <param name="dateTime"></param>
        public Timestamp(DateTime dateTime)
        {
            this.UnixTimeEpochTimestamp = (long)(dateTime - UnixTimeEpoch).TotalMilliseconds;
        }

        /// <summary>
        /// Creates a timestamp based in DateTime.UtcNow minus the UnixTimeEpoch (1970, 1, 1)
        /// </summary>
        public Timestamp()
        {
            this.UnixTimeEpochTimestamp = (long)(DateTime.UtcNow - UnixTimeEpoch).TotalMilliseconds;
        }

        public long UnixTimeEpochTimestamp { get; }
    }
}