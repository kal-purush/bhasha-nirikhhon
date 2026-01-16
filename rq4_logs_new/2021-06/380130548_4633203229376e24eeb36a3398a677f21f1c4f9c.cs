namespace Selectron.Testing.Ninject.SpecFlowPlugin.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Globalization;
    using System.Linq;
    using BoDi;
    using FluentAssertions;
    using global::Ninject;
    using global::Ninject.SpecFlowPlugin;
    using Moq;
    using NUnit.Framework;
    using SpecFlowPluginBase.Attributes;
    using SpecFlowPluginBase.ContainerLookup;
    using TechTalk.SpecFlow;
    using TechTalk.SpecFlow.Bindings;
    using TechTalk.SpecFlow.Bindings.Reflection;
    using TechTalk.SpecFlow.Configuration;
    using TechTalk.SpecFlow.Infrastructure;
    using TechTalk.SpecFlow.Plugins;
    using TechTalk.SpecFlow.UnitTestProvider;

    public class BaseFixture
    {
        private readonly SpecFlowConfiguration specFlowConfiguration = ConfigurationLoader.GetDefault();

        private IBindingRegistry bindingRegistry;

        protected ObjectContainer GlobalContainer { get; private set; }

        protected RuntimePluginEvents PluginEvents { get; private set; }

        [SetUp]
        public void SetUpBase()
        {
            this.bindingRegistry = Mock.Of<IBindingRegistry>();
            this.GlobalContainer = new ObjectContainer();
            this.GlobalContainer.RegisterInstanceAs(this.bindingRegistry);
            this.PluginEvents = new RuntimePluginEvents();
        }

        [TearDown]
        public void TearDownBase()
        {
            this.GlobalContainer.Dispose();
        }

        protected void AssociateRuntimeEventsWithPlugin<TScenarioContainerFinder, TFeatureContainerFinder,
            TTestThreadContainerFinder>(RuntimePluginEvents events)
            where TScenarioContainerFinder : ContainerSetupFinder<ScenarioDependenciesAttribute, IKernel>
            where TFeatureContainerFinder : ContainerSetupFinder<FeatureDependenciesAttribute, IKernel>
            where TTestThreadContainerFinder : ContainerSetupFinder<TestThreadDependenciesAttribute, IKernel>
        {
            var plugin = new NinjectPlugin();

            this.GlobalContainer.RegisterTypeAs<NinjectTestObjectResolver, ITestObjectResolver>();
            this.GlobalContainer
                .RegisterTypeAs<TScenarioContainerFinder, ContainerSetupFinder<ScenarioDependenciesAttribute, IKernel>>();
            this.GlobalContainer
                .RegisterTypeAs<TFeatureContainerFinder, ContainerSetupFinder<FeatureDependenciesAttribute, IKernel>>();
            this.GlobalContainer
                .RegisterTypeAs<TTestThreadContainerFinder, ContainerSetupFinder<TestThreadDependenciesAttribute, IKernel>>();

            plugin.Initialize(events, Mock.Of<RuntimePluginParameters>(), Mock.Of<UnitTestProviderConfiguration>());
            events.RaiseCustomizeGlobalDependencies(this.GlobalContainer, this.specFlowConfiguration);
        }

        protected IObjectContainer CreateTestThreadContainer(IObjectContainer parentContainer)
        {
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContextManager.cs#L159
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContainerBuilder.cs#L78
            var testThreadContainer = new ObjectContainer(parentContainer);
            this.PluginEvents.RaiseCustomizeTestThreadDependencies(testThreadContainer);

            return testThreadContainer;
        }

        protected IObjectContainer CreateFeatureContainer(IObjectContainer parentContainer)
        {
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContextManager.cs#L168
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContainerBuilder.cs#L112
            var featureContainer = new ObjectContainer(parentContainer);
            featureContainer.RegisterInstanceAs(this.specFlowConfiguration);
            featureContainer.RegisterInstanceAs(
                new FeatureInfo(CultureInfo.CurrentCulture, string.Empty, string.Empty, string.Empty));
            this.PluginEvents.RaiseCustomizeFeatureDependencies(featureContainer);

            return featureContainer;
        }

        protected IObjectContainer CreateScenarioContainer(IObjectContainer parentContainer)
        {
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContextManager.cs#L183
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContainerBuilder.cs#L90
            var scenarioContainer = new ObjectContainer(parentContainer);
            scenarioContainer.RegisterInstanceAs(
                new ScenarioInfo(string.Empty, string.Empty, Array.Empty<string>(), new OrderedDictionary()));
            this.PluginEvents.RaiseCustomizeScenarioDependencies(scenarioContainer);

            return scenarioContainer;
        }

        protected void SetupBindingRegistryWithAssemblyContainingHook(Type type)
        {
            var bindingRegistryMock = Mock.Get(this.bindingRegistry);
            var hookBinding = new Mock<IHookBinding>();
            var methodInfo = type.Methods().First();
            var bindingMethod = new RuntimeBindingMethod(methodInfo);
            hookBinding.SetupGet(binding => binding.Method).Returns(bindingMethod);
            bindingRegistryMock.Setup(registry => registry.GetHooks())
                .Returns(new List<IHookBinding> { hookBinding.Object });
        }
    }
}