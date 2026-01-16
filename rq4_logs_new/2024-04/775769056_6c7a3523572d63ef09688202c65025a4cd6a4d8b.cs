using System.Collections.Concurrent;

namespace LagDaemon.YAMUD.WebAPI.Services.ChatServices
{
    public static class ConnectionMapping<T>
    {
        private static readonly ConcurrentDictionary<T, HashSet<string>> _connections = new ConcurrentDictionary<T, HashSet<string>>();

        public static void Add(T key, string connectionId)
        {
            _connections.AddOrUpdate(key,
                // Add new key with a new HashSet containing the connectionId
                k => new HashSet<string> { connectionId },
                // Or update existing key by adding the connectionId to the HashSet
                (k, v) => { v.Add(connectionId); return v; });
        }

        public static void Remove(T key, string connectionId)
        {
            if (_connections.TryGetValue(key, out var connections))
            {
                connections.Remove(connectionId);
                if (connections.Count == 0)
                {
                    _connections.TryRemove(key, out var _);
                }
            }
        }

        public static IEnumerable<string> GetConnections(T key)
        {
            if (_connections.TryGetValue(key, out var connections))
            {
                return connections;
            }

            return Enumerable.Empty<string>();
        }
    }

}