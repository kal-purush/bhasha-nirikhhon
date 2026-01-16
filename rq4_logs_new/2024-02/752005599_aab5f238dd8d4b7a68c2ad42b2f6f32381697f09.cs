namespace Utils
{
    public static class Constants
    {
        public static Uri ZipkinEndpoint
        {
            get
            {
                var zipkinHostName = Environment.GetEnvironmentVariable("ZIPKIN_HOSTNAME") ?? "zipkin";
                return new Uri($"http://{zipkinHostName}:9411/api/v2/spans");
            }
        }

        public static string DatabaseConnectionString
        {
            get
            {
                var postgresHostName = Environment.GetEnvironmentVariable("POSTGRES_HOSTNAME") ?? "postgres";
                return $"Server={postgresHostName};Database=postgres;UserName=postgres;Password=password;";
            }
        }
    }
}