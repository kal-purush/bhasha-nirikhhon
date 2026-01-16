using System.Reflection;

namespace EasyReflection
{
    internal static class BindingFlagConstants
    {
        internal static BindingFlags AllFieldsBindingFlags = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public;

        internal static BindingFlags AllFieldsBindingFlagsStatic = BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Public;
    }
}