using ImageResizer.Configuration;
using ImageResizer.Configuration.Logging;
using log4net.Config;
using System.IO;
using System.Web;

namespace ImageResizer.Plugins.Log4net
{
    public class Log4netPlugin : ILogManager, IPlugin
    {
        private string _configFile;

        /// <summary>
        /// Uses the defaults from the resizing.azureBlobCache section in the specified configuration.
        /// </summary>
        public virtual void LoadSettings(Config config)
        {
            _configFile = config.get("log4net.configFile", _configFile);
        }

        public virtual IPlugin Install(Config config)
        {
            LoadSettings(config);
            LoadConfigFromFile(_configFile);

            config.Plugins.add_plugin(this);
            return this;
        }

        public virtual bool Uninstall(Config config)
        {
            config.Plugins.remove_plugin(this);
            return true;
        }

        public virtual ILogger GetLogger(string loggerName)
        {
            return new Log4netWrapper(loggerName);
        }

        public virtual void LoadConfigFromFile(string path)
        {
            var configFile = new FileInfo(ResolvePath(path));
            if (!configFile.Exists)
                return;

            XmlConfigurator.Configure(configFile);
        }

        private string ResolvePath(string filePath)
        {
            var context = HttpContext.Current;

            if (context != null)
            {
                try
                {
                    return context.Server.MapPath(filePath);
                }
                catch (HttpException)
                {
                    return filePath;
                }
            }
            else
            {
                return filePath;
            }
        }
    }
}