using System.Reflection;

namespace LangSharp.Utils
{
    public static class ResourceHelper
    {
        private const string LangSharpEmbeddedName = "LangSharp.Scripts";
        public static string ReadEmbeddedPythonScript(string scriptName)
        {
            var resourceName = $"{LangSharpEmbeddedName}.{scriptName}";


            var assembly = Assembly.GetExecutingAssembly();

            foreach (var name in assembly.GetManifestResourceNames())
            {
                Console.WriteLine(name);
            }

            using var stream = assembly.GetManifestResourceStream(resourceName);
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }
    }
}