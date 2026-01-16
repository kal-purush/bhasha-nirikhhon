using dreamlet.BusinessLogicLayer.Services.Interfaces;
using dreamlet.BusinessLogicLayer.Services.Providers;
using DryIoc;

namespace dreamlet.BusinessLogicLayer.Ioc
{
    public class IocBootstrapper
    {
        public static IContainer RegisterDependencies(IContainer container)
        {
            container.Register<IDreamStoriesService, DreamStoriesService>(Reuse.InWebRequest);
            container.Register<IDreamTermsService, DreamTermsService>(Reuse.InWebRequest);

            return container;
        }
    }
}