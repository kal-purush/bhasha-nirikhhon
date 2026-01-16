using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting.Server.Abstractions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using TS.StandaloneKestrelServer;
using TS.StandaloneKestrelServer.Extensions;
using Xunit;

namespace StandaloneKestrelServerTests
{
    public class ApplicationTests
    {
        [Fact]
        public void TestCreateContext()
        {
            var application = new Application((_) => Task.CompletedTask, new NullLoggerFactory());

            var featureCollection = new FeatureCollection();

            var context = application.CreateContext(featureCollection);

            Assert.NotNull(context);
            Assert.NotNull(context.HttpContext);
            Assert.NotNull(context.Container);

            Assert.IsType<DefaultHttpContext>(context.HttpContext);
            Assert.IsType<PersistentContainer>(context.Container);

            Assert.Equal(featureCollection, context.HttpContext.Features);
        }

        [Fact]
        public void TestCreateContextIsStoredInHttpContext()
        {
            var application = new Application((_) => Task.CompletedTask, new NullLoggerFactory());

            var featureCollection = new FeatureCollectionWithHostContextContainer();

            var context = application.CreateContext(featureCollection);

            Assert.NotNull(context);
            Assert.NotNull(context.HttpContext);
            Assert.NotNull(context.Container);

            Assert.Equal(featureCollection, context.HttpContext.Features);
            Assert.Equal(context, featureCollection.HostContext);
        }
        
        [Fact]
        public void TestCreateContextStoresPersistentContainerInHttpContext()
        {
            var application = new Application((_) => Task.CompletedTask, new NullLoggerFactory());

            var featureCollection = new FeatureCollection();
            var context = application.CreateContext(featureCollection);

            Assert.NotNull(context);
            Assert.NotNull(context.HttpContext);
            Assert.NotNull(context.Container);
         
            Assert.Equal(featureCollection, context.HttpContext.Features);
            Assert.Equal(context.Container, featureCollection.Get<PersistentContainer>());
        }

        [Fact]
        public void TestCreateContextReusesContextFromHostContextContainer()
        {
            var application = new Application((_) => Task.CompletedTask, new NullLoggerFactory());
            
            var featureCollection = new FeatureCollectionWithHostContextContainer();

            var context = application.CreateContext(featureCollection);

            Assert.NotNull(context);
            
            var context2 = application.CreateContext(featureCollection);
            
            Assert.Equal(context, context2);
            //TODO: test if initialize was called
        }
        
        [Fact]
        public async Task TestProcessRequestAsync()
        {
            RequestDelegate requestDelegate = httpContext =>
            {
                httpContext.Features.Get<PersistentContainer>().Set<string>("Test");
                return Task.CompletedTask;
            };
            
            var application = new Application(requestDelegate, new NullLoggerFactory());
            var context = application.CreateContext(new FeatureCollection());
            
            await application.ProcessRequestAsync(context);
            
            Assert.Equal("Test", context.Container.Get<string>());
        }
        
        
        [Fact]
        public async Task TestHttpContextExtensionGetPersistentContainer()
        {
            RequestDelegate requestDelegate = httpContext =>
            {
                Assert.NotNull(httpContext.GetPersistentContainer());
                httpContext.GetPersistentContainer()?.Set<string>("Test");
                return Task.CompletedTask;
            };
            
            var application = new Application(requestDelegate, new NullLoggerFactory());
            var context = application.CreateContext(new FeatureCollection());

            await application.ProcessRequestAsync(context);
            
            Assert.Equal("Test", context.Container.Get<string>());
        }


        private class FeatureCollectionWithHostContextContainer : IFeatureCollection,
            IHostContextContainer<Application.Context>
        {
            public object? this[Type key]
            {
                get => _featureCollection[key];
                set => _featureCollection[key] = value;
            }

            public Application.Context HostContext { get; set; } = null!;

            public bool IsReadOnly => false;

            public int Revision => 0;

            private readonly IFeatureCollection _featureCollection = new FeatureCollection();

            public TFeature Get<TFeature>()
            {
                return _featureCollection.Get<TFeature>();
            }

            public void Set<TFeature>(TFeature instance)
            {
                _featureCollection.Set(instance);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public IEnumerator<KeyValuePair<Type, object>> GetEnumerator()
            {
                return _featureCollection.GetEnumerator();
            }
        }
    }
}