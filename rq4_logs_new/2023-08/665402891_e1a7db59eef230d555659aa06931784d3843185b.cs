using Calendar.Api.Models;
using System.Reflection.Metadata;

namespace Calendar.Api.Configurations;

public class DebugEnvironmentConfiguration : IEnvironmentConfiguration<DebugEnvironmentConfiguration>
{
    public bool DEBUG_TEST_ENDPOINT_ENABLED { get; set; } = false;
    public string DEBUG_TEST_ENDPOINT_POLICY { get; set; }
    public DebugEnvironmentConfiguration Validate()
    {
        var validator = new EnvironmentConfigurationValidator()
            .CheckEnvironmentValue(DEBUG_TEST_ENDPOINT_ENABLED);
        if (DEBUG_TEST_ENDPOINT_ENABLED)
        {
            validator.CheckEnvironment(DEBUG_TEST_ENDPOINT_POLICY);
            switch (DEBUG_TEST_ENDPOINT_POLICY)
            {
                case AuthPolicies.EDITOR:
                case AuthPolicies.VIEWER:
                case AuthPolicies.EDITOR_VIEWER:
                    return this;
                default: 
                    throw new EnvironmentConfigurationException(nameof(DEBUG_TEST_ENDPOINT_POLICY), DEBUG_TEST_ENDPOINT_POLICY);
            }
        }
        return this;
    }
}