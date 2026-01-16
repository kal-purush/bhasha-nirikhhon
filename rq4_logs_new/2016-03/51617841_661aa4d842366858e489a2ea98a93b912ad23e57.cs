using System.ServiceModel;

// ReSharper disable once CheckNamespace
namespace Autofac
{
    public static class ContainerBuilderExtension
    {
        public static void RegisterWcfService<T>(this ContainerBuilder builder, string serviceUri)
        {
            builder.Register(c => new ChannelFactory<T>(new BasicHttpBinding(),
                new EndpointAddress(serviceUri))).SingleInstance();
            builder.Register(c => c.Resolve<ChannelFactory<T>>().CreateChannel()).As<T>();
        }
    }
}