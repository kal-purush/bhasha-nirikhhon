using Autofac;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Nameless.MongoDB.DependencyInjection {
    public sealed class Bootstrapper : IStartable {
        #region Private Read-Only Fields

        private readonly Type[] _mappings;

        #endregion

        #region Public Properties

        private ILogger? _logger;
        public ILogger Logger {
            get => _logger ??= NullLogger.Instance;
            set => _logger = value;
        }

        #endregion

        #region Public Constructors

        public Bootstrapper(Type[] mappings) {
            _mappings = mappings ?? Array.Empty<Type>();
        }

        #endregion

        #region IStartable Members

        public void Start() {
            foreach (var mapping in _mappings) {
                try { Activator.CreateInstance(mapping); }
                catch (Exception ex) { Logger.LogError(ex, "Error while running mapping {mapping}", mapping); }
            }
        }

        #endregion
    }
}