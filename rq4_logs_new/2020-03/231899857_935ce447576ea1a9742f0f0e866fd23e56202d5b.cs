using EventBus.Extensions.Microsoft.DependencyInjection.Settings;
using EventBus.Extensions.Microsoft.DependencyInjection.Settings.Abstract;
using EventBus.Extensions.Microsoft.DependencyInjection.Configurations.Abstract;

namespace EventBus.Extensions.Microsoft.DependencyInjection.Configurations
{
	public class ServiceBusConfiguration : IConfiguration
	{
		private readonly ServiceBusSettings _settings;

		public ServiceBusConfiguration()
		{
			_settings = new ServiceBusSettings
			{
				SubscriptionName = null,
			};
		}

		public ServiceBusConfiguration WithName(string subscriptionName)
		{
			_settings.SubscriptionName = subscriptionName;
			return this;
		}

		public ServiceBusConfiguration WithConnection(string connectionString)
		{
			_settings.ConnectionString = connectionString;
			return this;
		}

		ISettings IConfiguration.BuildSettings()
		{
			return _settings;
		}
	}
}