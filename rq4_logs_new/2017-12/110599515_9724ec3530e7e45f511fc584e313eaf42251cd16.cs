namespace PetProjects.Framework.Kafka.Contracts.Topics
{
    using System;

    public class TopicConfig
    {
        public TopicConfig(string entityName, MessageType messageType, string application, string environment, int version)
        {
            if (string.IsNullOrWhiteSpace(entityName))
            {
                throw new ArgumentException("EntityName must not be null or whitespace", nameof(entityName));
            }

            if (string.IsNullOrWhiteSpace(application))
            {
                throw new ArgumentException("Application must not be null or whitespace", nameof(application));
            }

            if (string.IsNullOrWhiteSpace(environment))
            {
                throw new ArgumentException("Environment must not be null or whitespace", nameof(environment));
            }

            if (version <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            this.EntityName = entityName.ToLowerInvariant();
            this.MessageType = messageType;
            this.Application = application.ToLowerInvariant();
            this.Environment = environment.ToLowerInvariant();
            this.Version = version;
        }

        public string EntityName { get; }

        public MessageType MessageType { get; }

        public string Application { get; }

        public string Environment { get; }

        public int Version { get; }
    }
}