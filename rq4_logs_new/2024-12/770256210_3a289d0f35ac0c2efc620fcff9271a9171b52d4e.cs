using Core.Abstractions.Attributes;
using Core.Abstractions.Configuration;
using Core.Abstractions.System;
using Core.Configuration;

namespace Core.Factories;
public static class ConfigurationFactory
{
    public static IConfigurationIO CreateCofigurationIO(IConfigurationState configurationState, IFileIO fileIo)
        => new ConfigurationIOImpl(configurationState, fileIo);

    public static IConfigurationState CreateConfigurationState(IAttributeExtracter attributeExtracter)
        => new ConfigurationStateImpl(attributeExtracter);
}