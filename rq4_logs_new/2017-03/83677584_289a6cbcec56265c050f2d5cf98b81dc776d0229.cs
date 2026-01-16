using System;
using Microsoft.Xrm.Sdk;

namespace KpdApps.Common.MsCrm2015
{
	public class PluginState
	{
		private IOrganizationService _service;

		private IPluginExecutionContext _context;

		public virtual IServiceProvider Provider { get; private set; }

		public virtual IPluginExecutionContext Context => _context ?? (_context = (IPluginExecutionContext)Provider.GetService(typeof(IPluginExecutionContext)));

		public virtual IOrganizationService Service
		{
			get
			{
				if (_service != null)
					return _service;

				IOrganizationServiceFactory factory = (IOrganizationServiceFactory)Provider.GetService(typeof(IOrganizationServiceFactory));
				_service = factory.CreateOrganizationService(this.Context.UserId);
				return _service;
			}
		}

		public PluginState(IServiceProvider provider)
		{
			if (provider == null)
				throw new ArgumentNullException(nameof(provider));

			Provider = provider;
		}

		public T GetTarget<T>() where T : class
		{
			if (Context.InputParameters.Contains("Target"))
			{
				return (T)Context.InputParameters["Target"];
			}

			return default(T); ;
		}
	}
}