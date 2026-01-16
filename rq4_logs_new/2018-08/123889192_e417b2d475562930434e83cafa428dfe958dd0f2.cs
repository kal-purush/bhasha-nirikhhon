using NUnit.Framework;
using UnityModule.AssetBundleManagement;
using UnityModule.ContextManagement;

namespace UnityModule.AssetBundleManagement
{
    public class URLResolverTest
    {
        private class MockProjectContext : IDownloadableProjectContext
        {
            public string Name { get; } = "TestName";
            public string SceneNamePrefix { get; } = "TestSceneName";
            public string NamespacePrefix { get; } = "TestNamespace";
            public int AssetBundleSingleManifestVersion { get; } = 1;
            public string InitialSceneName { get; set; }
            public IURLResolver AssetBundleURLResolverSingleManifest { get; set; }
            public IURLResolver AssetBundleURLResolver { get; set; }
            public string CreateSceneName<TEnum>(TEnum sceneName) where TEnum : struct
            {
                return $"{SceneNamePrefix.TrimEnd('_')}_{sceneName}";
            }
        }

        [SetUp]
        public void SetUp()
        {
            ContextManager.CurrentProject = new MockProjectContext();
        }

        [Test]
        public void AmazonS3URLResolverTest()
        {
            {
                var resolver = new AmazonS3URLResolver("ap-northeast-1", "test-bucket-1");
                var uri = resolver.Resolve();
                Assert.AreEqual("https", uri.Scheme);
                Assert.AreEqual("s3-ap-northeast-1.amazonaws.com", uri.Host);
                Assert.AreEqual("/test-bucket-1/AssetBundles/TestName/Standalone/SingleManifests/1", uri.AbsolutePath);
            }

            {
                var resolver = new AmazonS3URLResolver("ap-northeast-1", "test-bucket-2", true);
                var uri = resolver.Resolve();
                Assert.AreEqual("s3", uri.Scheme);
                Assert.AreEqual("test-bucket-2", uri.Host);
                Assert.AreEqual("/AssetBundles/TestName/Standalone/SingleManifests/1", uri.AbsolutePath);
            }
        }
    }
}