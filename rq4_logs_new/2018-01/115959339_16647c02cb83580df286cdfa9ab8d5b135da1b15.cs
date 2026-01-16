using Entities;
using ModelBuilder.Adapters;
using ModelBuilder.UseCases;

namespace ModelBuilder
{
    public static class ModelBuilderFeatureFactory
    {
        public static ModelBuilderFeature Create(IContainerBuilder components)
        {
            var pluginMgr = components.Get<IPluginManager>();
            var modelBuilderMgr = new ModelBuilderManager
            {
                AdditionalBuildersFunc = () => pluginMgr.Load<IModelBuilder>()
            };

            var controller = new ModelController(
                modelBuilderMgr,
                components.Get<Port<string>>(),
                components.Get<Port<string>>()
            );

            var device = components.Get<IDevice>();
            var cmd = new BuildModelCommand(device, components.Get<Port<string>>());
            cmd.OutPort.Connect(controller.InPort);

            controller.OutPort.OnDataSent += (sender, s) => device.WriteLine(s); // presenter and view for the poor

            var feature = new SimpleModelBuilderFeature(cmd);
            return feature;
        }
    }
}