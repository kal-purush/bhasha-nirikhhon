using System;
using Microsoft.Xrm.Sdk;

namespace KpdApps.Common.MsCrm2015
{
	public abstract class BasePlugin : IPlugin
	{
		public readonly string UnsecureConfiguration;

		public readonly string SecureConfiguration;

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="unsecureConfiguration"></param>
		/// <param name="secureConfiguration"></param>
		public BasePlugin(string unsecureConfiguration, string secureConfiguration)
		{
			UnsecureConfiguration = unsecureConfiguration;
			SecureConfiguration = secureConfiguration;
		}

		/// <summary>
		/// Constructor
		/// </summary>
		public BasePlugin()
		{

		}

		public void Execute(IServiceProvider serviceProvider)
		{
			throw new NotImplementedException();
		}
	}
}