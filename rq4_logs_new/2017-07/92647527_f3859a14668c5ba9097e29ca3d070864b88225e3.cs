using System;
using System.Threading.Tasks;
using MAF.FeaturesFlipping.Extensibility.Activators;
using Microsoft.Extensions.Configuration;

namespace MAF.FeaturesFlipping.Activators.Configuration
{
    internal class GlobalConfigurationFeature : IFeature
    {
        private readonly IConfigurationSection _configurationSection;

        public GlobalConfigurationFeature(IConfigurationSection configurationSection)
        {
            _configurationSection = configurationSection ??throw new ArgumentNullException(nameof(configurationSection));
        }

        public Task<FeatureActivationStatus> GetStatusAsync(IFeatureContext featureContext)
        {
            var value = _configurationSection.Value;
            if (string.IsNullOrEmpty(value))
            {
                return Task.FromResult(FeatureActivationStatus.NotSet);
            }

            if (bool.TryParse(value, out var isActive))
            {
                if (isActive)
                {
                    return Task.FromResult(FeatureActivationStatus.Active);
                }
                return Task.FromResult(FeatureActivationStatus.Inactive);
            }
            return Task.FromResult(FeatureActivationStatus.NotSet);
        }
    }
}