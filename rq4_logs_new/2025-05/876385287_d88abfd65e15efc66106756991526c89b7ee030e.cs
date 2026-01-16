using LangSharp.Core.Interfaces.Infrastructure;
using Python.Runtime;

namespace LangSharp.Core.Infrastructure
{
    public class PythonRuntime : IPythonRuntime
    {
        public bool IsInitialized => PythonEngine.IsInitialized;

        public void Initialize()
        {
            PythonEngine.Initialize();
            PythonEngine.BeginAllowThreads();
        }

        public IDisposable AcquireGIL()
        {
            return Py.GIL();
        }

        public PyObject Import(string moduleName)
        {
            return Py.Import(moduleName);
        }
    }
}