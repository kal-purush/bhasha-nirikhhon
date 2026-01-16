
namespace SharpBlueprint.Client
{
    public class Environment
    {
        public string GetFrameworkVersion()
        {
#if NETSTANDARD1_4
            return ".NET Standard 1.4";
#elif NET452
            return ".NET Framework 4.5.2";
#endif
        }
    }
}