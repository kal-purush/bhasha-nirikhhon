namespace Ninject.SpecFlowPlugin.Test
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using BoDi;
    using FluentAssertions;
    using Moq;
    using Ninject.SpecFlowPlugin.Attributes;
    using Ninject.SpecFlowPlugin.ContainerLookup;
    using Ninject.SpecFlowPlugin.Exceptions;
    using Ninject.SpecFlowPlugin.Test.TestObjects;
    using NoDependenciesAttribute.Hooks;
    using NUnit.Framework;
    using TechTalk.SpecFlow;
    using TechTalk.SpecFlow.Bindings;
    using TechTalk.SpecFlow.Bindings.Reflection;
    using TechTalk.SpecFlow.Configuration;
    using TechTalk.SpecFlow.Infrastructure;
    using TechTalk.SpecFlow.Plugins;
    using TechTalk.SpecFlow.Tracing;
    using TechTalk.SpecFlow.UnitTestProvider;
    using WrongAfterFeatureHookOrder.Hooks;
    using WrongAfterScenarioHookOrder.Hooks;
    using WrongAfterTestRunHookOrder.Hooks;

    [TestFixture]
    [SuppressMessage(
        "Design",
        "CA1001:Types that own disposable fields should be disposable",
        Justification = "disposed in tear down")]
    public class NinjectPluginFixture
    {
        private readonly SpecFlowConfiguration specFlowConfiguration = ConfigurationLoader.GetDefault();

        private IBindingRegistry bindingRegistry;

        private ObjectContainer globalContainer;

        private RuntimePluginEvents pluginEvents;

        [SetUp]
        public void SetUp()
        {
            this.bindingRegistry = Mock.Of<IBindingRegistry>();
            this.globalContainer = new ObjectContainer();
            this.globalContainer.RegisterInstanceAs(this.bindingRegistry);
            this.pluginEvents = new RuntimePluginEvents();
        }

        [TearDown]
        public void TearDown()
        {
            this.globalContainer.Dispose();
        }

        [Test]
        public void Can_Load_Plugin()
        {
            // Arrange
            var loader = new RuntimePluginLoader();
            var listener = Mock.Of<ITraceListener>();
            var pluginAssembly = Assembly.GetAssembly(typeof(NinjectPlugin));
            var pathToPluginDll = pluginAssembly.Location;

            // Act
            var plugin = loader.LoadPlugin(pathToPluginDll, listener);

            // Assert
            plugin.Should().NotBeNull();
        }

        [Test]
        public void Can_Resolve_Ninject_Kernel_From_ScenarioContainer()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var scenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                // Act
                var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                // Assert
                scenarioKernel.Should().NotBeNull();
            }
        }

        [Test]
        public void Can_Resolve_Ninject_Kernel_From_FeatureContainer()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                // Act
                var featureKernel = featureContainer.Resolve<IKernel>();

                // Assert
                featureKernel.Should().NotBeNull();
            }
        }

        [Test]
        public void Can_Resolve_Ninject_Kernel_From_TestThreadContainer()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                // Act
                var testThreadKernel = testThreadContainer.Resolve<IKernel>();

                // Assert
                testThreadKernel.Should().NotBeNull();
            }
        }

        [Test]
        public void Resolved_Scenario_Context_Is_The_Same_For_The_Same_Scenario_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var scenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                // Act
                var scenarioContext1 = scenarioKernel.Get<ScenarioContext>();
                var scenarioContext2 = scenarioKernel.Get<ScenarioContext>();

                // Assert
                scenarioContext2.Should().BeSameAs(scenarioContext1);
            }
        }

        [Test]
        public void ObjectContainer_Can_Be_Resolved_From_Scenario_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var expectedScenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                var scenarioKernel = expectedScenarioContainer.Resolve<IKernel>();

                // Act
                var actualScenarioContainer = scenarioKernel.Get<IObjectContainer>();

                // Assert
                actualScenarioContainer.Should().BeSameAs(expectedScenarioContainer);
            }
        }

        [Test]
        public void
            ObjectContainer_Resolved_From_Scenario_Kernel_Is_Not_Same_As_ObjectContainer_Resolved_From_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                using (var scenarioContainer = this.CreateScenarioContainer(featureContainer))
                {
                    var featureKernel = featureContainer.Resolve<IKernel>();
                    var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                    // Act
                    var objectContainerFromFeatureKernel = featureKernel.Get<IObjectContainer>();
                    var objectContainerFromScenarioKernel = scenarioKernel.Get<IObjectContainer>();

                    // Assert
                    objectContainerFromScenarioKernel.Should().NotBeSameAs(objectContainerFromFeatureKernel);
                }
            }
        }

        [Test]
        public void
            ObjectContainer_Resolved_From_Scenario_Kernel_Is_Not_Same_As_ObjectContainer_Resolved_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                using (var featureContainer = this.CreateFeatureContainer(testThreadContainer))
                {
                    using (var scenarioContainer = this.CreateScenarioContainer(featureContainer))
                    {
                        var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                        var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                        // Act
                        var objectContainerFromTestThreadKernel = testThreadKernel.Get<IObjectContainer>();
                        var objectContainerFromScenarioKernel = scenarioKernel.Get<IObjectContainer>();

                        // Assert
                        objectContainerFromScenarioKernel.Should().NotBeSameAs(objectContainerFromTestThreadKernel);
                    }
                }
            }
        }

        [Test]
        public void Scenario_Context_Cannot_Be_Resolved_From_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                var featureKernel = featureContainer.Resolve<IKernel>();

                // Act
                Action act = () => featureKernel.Get<ScenarioContext>();

                // Assert
                act.Should().Throw<ActivationException>();
            }
        }

        [Test]
        public void Scenario_Context_Cannot_Be_Resolved_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                var testThreadKernel = testThreadContainer.Resolve<IKernel>();

                // Act
                Action act = () => testThreadKernel.Get<ScenarioContext>();

                // Assert
                act.Should().Throw<ActivationException>();
            }
        }

        [Test]
        public void Scenario_Kernel_Can_Resolve_Object_From_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                using (var scenarioContainer = this.CreateScenarioContainer(featureContainer))
                {
                    var featureKernel = featureContainer.Resolve<IKernel>();
                    featureKernel.Bind<ITestClass>().To<TestClass>();
                    var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                    // Act
                    Action act = () => scenarioKernel.Get<ITestClass>();

                    // Assert
                    act.Should().NotThrow();
                }
            }
        }

        [Test]
        public void Scenario_Kernel_Can_Resolve_Object_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                using (var scenarioContainer = this.CreateScenarioContainer(testThreadContainer))
                {
                    var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                    testThreadKernel.Bind<ITestClass>().To<TestClass>();
                    var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                    // Act
                    Action act = () => scenarioKernel.Get<ITestClass>();

                    // Assert
                    act.Should().NotThrow();
                }
            }
        }

        [Test]
        public void Resolved_Feature_Context_Is_The_Same_For_The_Same_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                // Act
                var featureKernel = featureContainer.Resolve<IKernel>();
                var featureContext1 = featureKernel.Get<FeatureContext>();
                var featureContext2 = featureKernel.Get<FeatureContext>();

                // Assert
                featureContext2.Should().BeSameAs(featureContext1);
            }
        }

        [Test]
        public void Feature_Context_Can_Be_Resolved_From_Scenario_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                using (var scenarioContainer = this.CreateScenarioContainer(featureContainer))
                {
                    var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                    // Act
                    Action act = () => scenarioKernel.Get<FeatureContext>();

                    // Assert
                    act.Should().NotThrow();
                }
            }
        }

        [Test]
        public void Feature_Context_Can_Be_Resolved_From_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                var featureKernel = featureContainer.Resolve<IKernel>();

                // Act
                Action act = () => featureKernel.Get<FeatureContext>();

                // Assert
                act.Should().NotThrow();
            }
        }

        [Test]
        public void Resolved_Feature_Context_Is_The_Same_In_Both_Scenario_Kernel_And_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                using (var scenarioContainer = this.CreateScenarioContainer(featureContainer))
                {
                    // Act
                    var featureKernel = featureContainer.Resolve<IKernel>();
                    var scenarioKernel = scenarioContainer.Resolve<IKernel>();
                    var featureContextFromFeatureKernel = featureKernel.Get<FeatureContext>();
                    var featureContextFromScenarioKernel = scenarioKernel.Get<FeatureContext>();

                    // Assert
                    featureContextFromFeatureKernel.Should().BeSameAs(featureContextFromScenarioKernel);
                }
            }
        }

        [Test]
        public void Feature_Contexts_Persist_Across_Scenarios()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                using (var scenarioContainer1 = this.CreateScenarioContainer(featureContainer))
                {
                    using (var scenarioContainer2 = this.CreateScenarioContainer(featureContainer))
                    {
                        // Act
                        var featureKernel = featureContainer.Resolve<IKernel>();
                        var featureContextFromFeatureKernel = featureKernel.Get<FeatureContext>();
                        var scenarioKernel1 = scenarioContainer1.Resolve<IKernel>();
                        var featureContextFromScenarioKernel1 = scenarioKernel1.Get<FeatureContext>();
                        var scenarioKernel2 = scenarioContainer2.Resolve<IKernel>();
                        var featureContextFromScenarioKernel2 = scenarioKernel2.Get<FeatureContext>();

                        // Assert
                        featureContextFromFeatureKernel.Should().BeSameAs(featureContextFromScenarioKernel1);
                        featureContextFromScenarioKernel1.Should().BeSameAs(featureContextFromScenarioKernel2);
                    }
                }
            }
        }

        [Test]
        public void Feature_Context_Cannot_Be_Resolved_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                var testThreadKernel = testThreadContainer.Resolve<IKernel>();

                // Act
                Action act = () => testThreadKernel.Get<FeatureContext>();

                // Assert
                act.Should().Throw<ActivationException>();
            }
        }

        [Test]
        public void ObjectContainer_Can_Be_Resolved_From_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var expectedFeatureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                var featureKernel = expectedFeatureContainer.Resolve<IKernel>();

                // Act
                var actualFeatureContainer = featureKernel.Get<IObjectContainer>();

                // Assert
                actualFeatureContainer.Should().BeSameAs(expectedFeatureContainer);
            }
        }

        [Test]
        public void
            ObjectContainer_Resolved_From_Feature_Kernel_Is_Not_Same_As_ObjectContainer_Resolved_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                using (var featureContainer = this.CreateFeatureContainer(testThreadContainer))
                {
                    var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                    var featureKernel = featureContainer.Resolve<IKernel>();

                    // Act
                    var objectContainerFromFeatureKernel = featureKernel.Get<IObjectContainer>();
                    var objectContainerFromTestThreadKernel = testThreadKernel.Get<IObjectContainer>();

                    // Assert
                    objectContainerFromTestThreadKernel.Should().NotBeSameAs(objectContainerFromFeatureKernel);
                }
            }
        }

        [Test]
        public void Feature_Kernel_Can_Resolve_Object_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                using (var featureContainer = this.CreateFeatureContainer(testThreadContainer))
                {
                    var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                    testThreadKernel.Bind<ITestClass>().To<TestClass>();
                    var featureKernel = featureContainer.Resolve<IKernel>();

                    // Act
                    Action act = () => featureKernel.Get<ITestClass>();

                    // Assert
                    act.Should().NotThrow();
                }
            }
        }

        [Test]
        public void Resolved_TestThread_Context_Is_The_Same_For_The_Same_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                // Act
                var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                var testThreadContext1 = testThreadKernel.Get<TestThreadContext>();
                var testThreadContext2 = testThreadKernel.Get<TestThreadContext>();

                // Assert
                testThreadContext2.Should().BeSameAs(testThreadContext1);
            }
        }

        [Test]
        public void TestThread_Context_Can_Be_Resolved_From_Scenario_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                using (var featureContainer = this.CreateFeatureContainer(testThreadContainer))
                {
                    using (var scenarioContainer = this.CreateScenarioContainer(featureContainer))
                    {
                        var scenarioKernel = scenarioContainer.Resolve<IKernel>();

                        // Act
                        Action act = () => scenarioKernel.Get<TestThreadContext>();

                        // Assert
                        act.Should().NotThrow();
                    }
                }
            }
        }

        [Test]
        public void TestThread_Context_Can_Be_Resolved_From_Feature_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                using (var featureContainer = this.CreateFeatureContainer(testThreadContainer))
                {
                    var featureKernel = featureContainer.Resolve<IKernel>();

                    // Act
                    Action act = () => featureKernel.Get<TestThreadContext>();

                    // Assert
                    act.Should().NotThrow();
                }
            }
        }

        [Test]
        public void TestThread_Context_Can_Be_Resolved_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                var testThreadKernel = testThreadContainer.Resolve<IKernel>();

                // Act
                Action act = () => testThreadKernel.Get<TestThreadContext>();

                // Assert
                act.Should().NotThrow();
            }
        }

        [Test]
        public void
            Resolved_TestThread_Context_Is_The_Same_In_Scenario_Kernel_And_Feature_Kernel_And_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                using (var featureContainer = this.CreateFeatureContainer(testThreadContainer))
                {
                    using (var scenarioContainer = this.CreateScenarioContainer(featureContainer))
                    {
                        // Act
                        var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                        var featureKernel = featureContainer.Resolve<IKernel>();
                        var scenarioKernel = scenarioContainer.Resolve<IKernel>();
                        var testThreadContextFromTestThreadKernel = testThreadKernel.Get<TestThreadContext>();
                        var testThreadContextFromFeatureKernel = featureKernel.Get<TestThreadContext>();
                        var testThreadContextFromScenarioKernel = scenarioKernel.Get<TestThreadContext>();

                        // Assert
                        testThreadContextFromTestThreadKernel.Should().BeSameAs(testThreadContextFromScenarioKernel);
                        testThreadContextFromTestThreadKernel.Should().BeSameAs(testThreadContextFromFeatureKernel);
                    }
                }
            }
        }

        [Test]
        public void TestThread_Contexts_Persist_Across_Scenarios()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                using (var featureContainer = this.CreateFeatureContainer(testThreadContainer))
                {
                    using (var scenarioContainer1 = this.CreateScenarioContainer(featureContainer))
                    {
                        using (var scenarioContainer2 = this.CreateScenarioContainer(featureContainer))
                        {
                            // Act
                            var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                            var testThreadContextFromTestThreadKernel = testThreadKernel.Get<TestThreadContext>();
                            var scenarioKernel1 = scenarioContainer1.Resolve<IKernel>();
                            var testThreadContextFromScenarioKernel1 = scenarioKernel1.Get<TestThreadContext>();
                            var scenarioKernel2 = scenarioContainer2.Resolve<IKernel>();
                            var testThreadContextFromScenarioKernel2 = scenarioKernel2.Get<TestThreadContext>();

                            // Assert
                            testThreadContextFromTestThreadKernel.Should()
                                .BeSameAs(testThreadContextFromScenarioKernel1);
                            testThreadContextFromTestThreadKernel.Should()
                                .BeSameAs(testThreadContextFromScenarioKernel2);
                        }
                    }
                }
            }
        }

        [Test]
        public void TestThread_Contexts_Persist_Across_Features()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                using (var featureContainer1 = this.CreateFeatureContainer(testThreadContainer))
                {
                    using (var featureContainer2 = this.CreateFeatureContainer(testThreadContainer))
                    {
                        // Act
                        var testThreadKernel = testThreadContainer.Resolve<IKernel>();
                        var testThreadContextFromTestThreadKernel = testThreadKernel.Get<TestThreadContext>();
                        var featureKernel1 = featureContainer1.Resolve<IKernel>();
                        var testThreadContextFromFeatureKernel1 = featureKernel1.Get<TestThreadContext>();
                        var featureKernel2 = featureContainer2.Resolve<IKernel>();
                        var testThreadContextFromFeatureKernel2 = featureKernel2.Get<TestThreadContext>();

                        // Assert
                        testThreadContextFromTestThreadKernel.Should().BeSameAs(testThreadContextFromFeatureKernel1);
                        testThreadContextFromTestThreadKernel.Should().BeSameAs(testThreadContextFromFeatureKernel2);
                    }
                }
            }
        }

        [Test]
        public void ObjectContainer_Can_Be_Resolved_From_TestThread_Kernel()
        {
            // Arrange
            this.AssociateRuntimeEventsWithPlugin<NoOpContainerFinder<ScenarioDependenciesAttribute>,
                NoOpContainerFinder<FeatureDependenciesAttribute>,
                NoOpContainerFinder<TestThreadDependenciesAttribute>>(this.pluginEvents);
            using (var expectedTestThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                var testThreadKernel = expectedTestThreadContainer.Resolve<IKernel>();

                // Act
                var actualTestThreadContainer = testThreadKernel.Get<IObjectContainer>();

                // Assert
                actualTestThreadContainer.Should().BeSameAs(expectedTestThreadContainer);
            }
        }

        [Test]
        public void Throws_When_No_ScenarioDependenciesAttribute_Is_Found_In_Binding_Assemblies()
        {
            // Arrange
            this.SetupBindingRegistryWithAssemblyContainingHook(typeof(NoOpHook));
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var scenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                // Act
                Action act = () => scenarioContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.ScenarioDependenciesNotFound);
            }
        }

        [Test]
        public void Does_Not_Throw_When_No_FeatureDependenciesAttribute_Is_Found_In_Binding_Assemblies()
        {
            // Arrange
            this.SetupBindingRegistryWithAssemblyContainingHook(typeof(NoOpHook));
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                // Act
                Action act = () => featureContainer.Resolve<IKernel>();

                // Assert
                act.Should().NotThrow();
            }
        }

        [Test]
        public void Does_Not_Throw_When_No_TestThreadDependenciesAttribute_Is_Found_In_Binding_Assemblies()
        {
            // Arrange
            this.SetupBindingRegistryWithAssemblyContainingHook(typeof(NoOpHook));
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                // Act
                Action act = () => testThreadContainer.Resolve<IKernel>();

                // Assert
                act.Should().NotThrow();
            }
        }

        [TestCase(
            typeof(WrongReturnTypeScenarioDependencies.Hooks.NoOpHook),
            Description = "The DependenciesConfigurator.SetupScenarioContainer method does not return void.")]
        [TestCase(
            typeof(WrongInputArgTypeScenarioDependencies.Hooks.NoOpHook),
            Description =
                "The DependenciesConfigurator.SetupScenarioContainer method's argument is not of type IKernel.")]
        [TestCase(
            typeof(WrongAmountInputArgsScenarioDependencies.Hooks.NoOpHook),
            Description =
                "The DependenciesConfigurator.SetupScenarioContainer method does not take one single input argument.")]
        public void Throws_When_Wrong_Scenario_Container_Setup_Method_Signature(Type bindingClassType)
        {
            // Arrange
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var scenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                // Act
                Action act = () => scenarioContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.WrongDependenciesSetupMethodSignature);
            }
        }

        [TestCase(
            typeof(WrongReturnTypeFeatureDependencies.Hooks.NoOpHook),
            Description = "The DependenciesConfigurator.SetupFeatureContainer method does not return void.")]
        [TestCase(
            typeof(WrongInputArgTypeFeatureDependencies.Hooks.NoOpHook),
            Description =
                "The DependenciesConfigurator.SetupFeatureContainer method's argument is not of type IKernel.")]
        [TestCase(
            typeof(WrongAmountInputArgsFeatureDependencies.Hooks.NoOpHook),
            Description =
                "The DependenciesConfigurator.SetupFeatureContainer method does not take one single input argument.")]
        public void Throws_When_Wrong_Feature_Container_Setup_Method_Signature(Type bindingClassType)
        {
            // Arrange
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                // Act
                Action act = () => featureContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.WrongDependenciesSetupMethodSignature);
            }
        }

        [TestCase(
            typeof(WrongReturnTypeTestThreadDependencies.Hooks.NoOpHook),
            Description = "The DependenciesConfigurator.SetupTestThreadContainer method does not return void.")]
        [TestCase(
            typeof(WrongInputArgTypeTestThreadDependencies.Hooks.NoOpHook),
            Description =
                "The DependenciesConfigurator.SetupTestThreadContainer method's argument is not of type IKernel.")]
        [TestCase(
            typeof(WrongAmountInputArgsTestThreadDependencies.Hooks.NoOpHook),
            Description =
                "The DependenciesConfigurator.SetupTestThreadContainer method does not take one single input argument.")]
        public void Throws_When_Wrong_TestThread_Container_Setup_Method_Signature(Type bindingClassType)
        {
            // Arrange
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                // Act
                Action act = () => testThreadContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.WrongDependenciesSetupMethodSignature);
            }
        }

        [Test]
        public void Throws_When_After_Scenario_Hook_Found_With_Too_High_Order_During_ScenarioContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterScenarioHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var scenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                // Act
                Action act = () => scenarioContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_Scenario_Hook_Found_With_Too_High_Order_During_FeatureContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterScenarioHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                // Act
                Action act = () => featureContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_Scenario_Hook_Found_With_Too_High_Order_During_TestThreadContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterScenarioHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                // Act
                Action act = () => testThreadContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_Feature_Hook_Found_With_Too_High_Order_During_ScenarioContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterFeatureHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var scenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                // Act
                Action act = () => scenarioContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_Feature_Hook_Found_With_Too_High_Order_During_FeatureContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterFeatureHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                // Act
                Action act = () => featureContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_Feature_Hook_Found_With_Too_High_Order_During_TestThreadContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterFeatureHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                // Act
                Action act = () => testThreadContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_TestRun_Hook_Found_With_Too_High_Order_During_ScenarioContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterTestRunHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var scenarioContainer = this.CreateScenarioContainer(this.globalContainer))
            {
                // Act
                Action act = () => scenarioContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_TestRun_Hook_Found_With_Too_High_Order_During_FeatureContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterTestRunHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var featureContainer = this.CreateFeatureContainer(this.globalContainer))
            {
                // Act
                Action act = () => featureContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        [Test]
        public void Throws_When_After_TestRun_Hook_Found_With_Too_High_Order_During_TestThreadContainer_Creation()
        {
            // Arrange
            var bindingClassType = typeof(TooHighOrderAfterTestRunHooks);
            this.SetupBindingRegistryWithAssemblyContainingHook(bindingClassType);
            this.AssociateRuntimeEventsWithPlugin<ScenarioContainerFinder, FeatureContainerFinder,
                TestThreadContainerFinder>(this.pluginEvents);
            using (var testThreadContainer = this.CreateTestThreadContainer(this.globalContainer))
            {
                // Act
                Action act = () => testThreadContainer.Resolve<IKernel>();

                // Assert
                act.Should()
                    .Throw<TargetInvocationException>()
                    .WithInnerException<TargetInvocationException>()
                    .WithInnerException<NinjectPluginException>()
                    .And.Error.Should()
                    .Be(NinjectPluginError.IncompatibleHookFound);
            }
        }

        private void AssociateRuntimeEventsWithPlugin<TScenarioContainerFinder, TFeatureContainerFinder,
            TTestThreadContainerFinder>(RuntimePluginEvents events)
            where TScenarioContainerFinder : ContainerFinder<ScenarioDependenciesAttribute>
            where TFeatureContainerFinder : ContainerFinder<FeatureDependenciesAttribute>
            where TTestThreadContainerFinder : ContainerFinder<TestThreadDependenciesAttribute>
        {
            var plugin = new NinjectPlugin();

            this.globalContainer.RegisterTypeAs<NinjectTestObjectResolver, ITestObjectResolver>();
            this.globalContainer
                .RegisterTypeAs<TScenarioContainerFinder, ContainerFinder<ScenarioDependenciesAttribute>>();
            this.globalContainer
                .RegisterTypeAs<TFeatureContainerFinder, ContainerFinder<FeatureDependenciesAttribute>>();
            this.globalContainer
                .RegisterTypeAs<TTestThreadContainerFinder, ContainerFinder<TestThreadDependenciesAttribute>>();

            plugin.Initialize(events, Mock.Of<RuntimePluginParameters>(), Mock.Of<UnitTestProviderConfiguration>());
            events.RaiseCustomizeGlobalDependencies(this.globalContainer, this.specFlowConfiguration);
        }

        private IObjectContainer CreateTestThreadContainer(IObjectContainer parentContainer)
        {
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContextManager.cs#L159
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContainerBuilder.cs#L78
            var testThreadContainer = new ObjectContainer(parentContainer);
            this.pluginEvents.RaiseCustomizeTestThreadDependencies(testThreadContainer);

            return testThreadContainer;
        }

        private IObjectContainer CreateFeatureContainer(IObjectContainer parentContainer)
        {
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContextManager.cs#L168
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContainerBuilder.cs#L112
            var featureContainer = new ObjectContainer(parentContainer);
            featureContainer.RegisterInstanceAs(this.specFlowConfiguration);
            featureContainer.RegisterInstanceAs(
                new FeatureInfo(CultureInfo.CurrentCulture, string.Empty, string.Empty, string.Empty));
            this.pluginEvents.RaiseCustomizeFeatureDependencies(featureContainer);

            return featureContainer;
        }

        private IObjectContainer CreateScenarioContainer(IObjectContainer parentContainer)
        {
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContextManager.cs#L183
            // cf. https://github.com/SpecFlowOSS/SpecFlow/blob/1f15a7480ef0dbf7a432f01bdccae4fe12fb6539/TechTalk.SpecFlow/Infrastructure/ContainerBuilder.cs#L90
            var scenarioContainer = new ObjectContainer(parentContainer);
            scenarioContainer.RegisterInstanceAs(
                new ScenarioInfo(string.Empty, string.Empty, Array.Empty<string>(), new OrderedDictionary()));
            this.pluginEvents.RaiseCustomizeScenarioDependencies(scenarioContainer);

            return scenarioContainer;
        }

        private void SetupBindingRegistryWithAssemblyContainingHook(Type type)
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