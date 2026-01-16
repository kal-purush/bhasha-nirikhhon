using FluentResults;
using IronPython.Hosting;
using Microsoft.Scripting.Hosting;
using System.Linq.Expressions;

namespace LagDaemon.YAMUD.Automation
{
    public class IronPythonExecutor : IDisposable
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ScriptEngine _pythonEngine;
        private readonly ScriptScope _pythonScope;
        private string? _script = null;

        public IronPythonExecutor(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _pythonEngine = Python.CreateEngine();
            _pythonScope = _pythonEngine.CreateScope();
            _pythonScope.SetVariable("services", _serviceProvider);
        }

        public ScriptScope ScriptScope => _pythonScope;

        public void Load(string script)
        {
            _script = script;
        }

        public Result<bool> ExecuteScript()
        {
            Exception _ex = null;
            var result = ThreadPool.QueueUserWorkItem(state =>
            {
                try
                {
                    _pythonEngine.Execute(_script, _pythonScope);
                } catch (Exception ex)
                {
                    _ex = ex;
                }
            });

            if (result)
            {
                return Result.Ok(true);
            } else
            {
                return Result.Fail<bool>(_ex.Message);
            }
        }

        public void Dispose()
        {
            _pythonEngine?.Runtime.Shutdown();
        }
    }
}
