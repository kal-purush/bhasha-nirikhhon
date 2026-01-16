using Caliburn.Micro;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nt.WpfClient.Utils.Bootstrap
{
    public class BootstrapLogger : ILog
    {
        private readonly Type _type;

        public BootstrapLogger(Type type)
        {
            _type = type;
        }

        public void Info(string format, params object[] args)
        {
            if (format.StartsWith("No bindable"))
                return;
            if (format.StartsWith("Action Convention Not Applied"))
                return;
            Debug.WriteLine("INFO: " + format, args);
        }

        public void Warn(string format, params object[] args)
        {
            Debug.WriteLine("WARN: " + format, args);
        }

        public void Error(Exception exception)
        {
            Debug.WriteLine("ERROR: {0}\n{1}", _type.Name, exception);
        }
    }
}