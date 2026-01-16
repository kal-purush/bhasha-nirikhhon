namespace JobBoard.Tests.Integration
{
    /*
     Added because tests were failing when there was too many containers running at once(i.e. one container per class),
     so i added collections to limit number of concurrent containers
     */
    internal static class Collections
    {
        public const string ApiCollection1 = "Api1";
        public const string ApiCollection2 = "Api2";
        public const string ApiCollection3 = "Api3";
        public const string ApiCollection4 = "Api4";
        public const string ApiCollection5 = "Api5";
        public const string RepositoryCollection1 = "Repository1";
        public const string RepositoryCollection2 = "Repository2";
    }
}