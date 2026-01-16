using Python.Included;
using Python.Runtime;

namespace PyIncluded.Drive
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            await Installer.SetupPython();
            PythonEngine.Initialize();
            dynamic sys = Py.Import("sys");
            Console.WriteLine("Python version: " + sys.version);

            await Installer.TryInstallPip();
            await Installer.PipInstallModule("spacy");

            dynamic spacy = Py.Import("spacy");
            Console.WriteLine("Spacy version: " + spacy.__version__);
        }
    }
}