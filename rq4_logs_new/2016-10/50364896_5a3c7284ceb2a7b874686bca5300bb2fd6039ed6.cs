namespace NServiceBus
{
    using NServiceBus.Configuration.AdvanceExtensibility;

    /// <summary>
    /// Plugin extension methods.
    /// </summary>
    public static class CustomCheckPluginExtensions
    {
        /// <summary>
        /// Sets the ServiceControl queue address.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="serviceControlQueue">ServiceControl queue address.</param>
        public static void CustomCheckPlugin(this EndpointConfiguration config, string serviceControlQueue)
        {
            config.GetSettings().Set("ServiceControl.Queue", serviceControlQueue);
        }
    }
}