namespace Entities
{
    public interface ILogger
    {
        void WriteInfo(string s);
    }

    public static class Logger
    {
        private static ILogger _logger;
        private static volatile object _syncRoot = new object();

        public static void Set(ILogger logger)
        {
            if (_logger == null)
            {
                lock (_syncRoot)
                {
                    if (_logger == null)
                    {
                        _logger = logger;
                    }
                }
            }
        }

        public static void WriteInfo(string s)
        {
            _logger?.WriteInfo(s);
        }
    }
}