namespace PetProjects.Framework.Kafka.Exceptions
{
    public static class ExceptionMessages
    {
        public static class Common
        {
            public const string InvalidBoostrapServers = "There is no bootstrap server configured. Please add at least one.";

            public const string InvalidClientIdInput = "ClientId cannot be null or whitespace. Please fix.";

            public const string InvalidGroupIdInput = "GroupId cannot be null or whitespace. Please fix.";
        }

        public static class ProducerErrorMessages
        {
            public const string InvalidRetriesIdempotence = "Retries must be greater than 0.";

            public const string InvalidBatchSizeInput = "BatchSize must be greater than 0.";

            public static string InvalidMaxInFlightRequestPerConnectionIdempotence(int maxRequest)
            {
                return $"MaxInFlightRequestPerConnection must be less than or equal to {maxRequest}.";
            }
        }

        public static class ConsumerErrorMessages
        {
            public const string InvalidIntervalInput = "Interval must be greater than 0.";

            public const string InvalidPollIntervalInput = "PollInterval must be greater than 0.";

            public const string CouldNotDisposeConsumer = "Could dispose consumer";
        }
    }
}