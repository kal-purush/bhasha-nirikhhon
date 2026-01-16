using System;

namespace HOLMS.Messaging {
    public class RabbitConfiguration {
        public string Host { get; protected set; }
        public ushort Port { get; protected set; }
        public string User { get; protected set; }
        public string Password { get; protected set; }
        public string VHost { get; protected set; }

        public RabbitConfiguration(string connectionString) {
            //example: amqp://user:pass@hostname:port/vhost
            int protocolEndIndex = connectionString.IndexOf("://");
            if (protocolEndIndex == -1) {
                throw new ArgumentException($"No protocol specified in string {connectionString}");
            }
            var protocol = connectionString.Substring(0, protocolEndIndex);
            if (protocol != "amqp" && protocol != "amqps") {
                throw new ArgumentException($"Invalid protocol {protocol}");
            }
            var connection = connectionString.Substring(protocolEndIndex + "://".Length);
            if (connection.Contains("@")) {
                var credentials = connection.Split('@')[0].Split(':');
                User = credentials[0];
                if (credentials.Length > 1) {
                    Password = credentials[1];
                }
                if (credentials.Length > 2) {
                    throw new ArgumentException($"Invalid field following password: {credentials[2]}. (Note: username / password may not contain a ':'");
                }
                //strip the credentials from the string. Assumes only one "@" in the entire string
                connection = connection.Split('@')[1];
            }
            var lastColon = connection.LastIndexOf(':');
            Host = connection.Substring(0, lastColon);
            var portAndVHost = connection.Substring(lastColon + 1).Split('/');
            
            ushort tcpPort;
            if (!ushort.TryParse(portAndVHost[0], out tcpPort)) {
                throw new ArgumentException($"Failed to parse PBXConnectionString port {tcpPort} as valid port number (integer between 1 and 65535");
            }
            Port = tcpPort;

            if (portAndVHost.Length > 1) {
                VHost = portAndVHost[1];
            }
        }
    }
}