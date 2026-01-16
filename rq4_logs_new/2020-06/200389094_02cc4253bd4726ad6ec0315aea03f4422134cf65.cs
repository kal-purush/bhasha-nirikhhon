using Autofac;
using CommonDomainObjects;
using Microsoft.AspNetCore.Mvc;
using NHibernate;
using NHibernate.Tool.hbm2ddl;
using NHibernateIntegration;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Web;
using Web.Controllers;

namespace Test
{
    [TestFixture]
    public class TestClassificationSchemeController
    {
        private static readonly IDictionary<char, IList<char>> _super = new Dictionary<char, IList<char>>
        {
            { 'A', new char[]{} },
            { 'B', new char[]{} },
            { 'C', new char[]{ 'B' } },
            { 'D', new char[]{ 'B' } },
        };

        private IContainer _container;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var builder = new ContainerBuilder();
            builder
                .RegisterModule<NHibernateIntegration.Module>();
            builder
                .RegisterType<CommonDomainObjects.Mapping.ConventionModelMapperFactory>()
                .As<IModelMapperFactory>()
                .SingleInstance();
            builder
                .RegisterModule(new SQLiteModule("Test"));
            builder
                .RegisterModule<Service.Module>();
            builder
                .RegisterModule<ControllerModule>();
            builder
                .RegisterModule<MapperModule>();

            _container = builder.Build();

            File.Delete(SQLiteModule.DatabasePath);
            var schemaExport = new SchemaExport(_container.Resolve<IConfigurationFactory>().Build());
            schemaExport.Create(
                scriptAction => { },
                true);
        }


        [SetUp]
        public void SetUp()
        {
            using(var scope = _container.BeginLifetimeScope())
            {
                var session = scope.Resolve<ISession>();
                session.CreateQuery("delete ClassificationSchemeClass").ExecuteUpdate();
                session.CreateQuery("delete CommonDomainObjects.Class").ExecuteUpdate();
                session.CreateQuery("delete ClassificationScheme").ExecuteUpdate();
            }
        }

        [Test]
        public async Task Get()
        {
            var super = _super.Select(
                vertex => new Class(
                    Guid.NewGuid(),
                    vertex.ToString()));

            var classificationScheme = new ClassificationScheme(super);
            using(var scope = _container.BeginLifetimeScope())
            {
                var session = scope.Resolve<ISession>();
                await session.SaveAsync(classificationScheme);
                await classificationScheme.VisitAsync(
                    async classificationSchemeClass =>
                    {
                        await session.SaveAsync(classificationSchemeClass.Class);
                        await session.SaveAsync(classificationSchemeClass);
                    },
                    null);
                await session.FlushAsync();
            }

            Web.Model.ClassificationScheme classificationSchemeModel = null;
            using(var scope = _container.BeginLifetimeScope())
            {
                var controller = scope.Resolve<ClassificationSchemeController>();
                var actionResult = await controller.GetAsync(classificationScheme.Id);
                Assert.That(actionResult, Is.Not.Null);
                Assert.That(actionResult, Is.InstanceOf<OkObjectResult>());
                var okObjectResult = (OkObjectResult)actionResult;
                Assert.That(okObjectResult.Value, Is.InstanceOf<Web.Model.ClassificationScheme>());
                classificationSchemeModel = (Web.Model.ClassificationScheme)okObjectResult.Value;
                Assert.That(classificationSchemeModel.Id, Is.EqualTo(classificationScheme.Id));
            }

            //foreach(var classificationSchemeClassModel in classificationSchemeModel.Classes)
            //{
            //    var classificationSchemeClass = classificationScheme[classificationSchemeClassModel.Class];
            //    Assert.That(classificationSchemeClass, Is.Not.Null);
            //    Assert.That(classificationSchemeClassModel.Id, Is.EqualTo(classificationSchemeClass.Id));
            //    Assert.That(classificationSchemeClassModel.Super, Is.EqualTo(classificationSchemeClass.Super));
            //}
        }
    }
}