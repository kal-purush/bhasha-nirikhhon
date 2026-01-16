using System.Linq;
using Xunit;

namespace LazyStorage.Tests.Xml
{
    public class XmlFactoryTests
    {
        [Fact]
        public void CanGetXmlStorage()
        {
            var dal = new StorageFactory().GetXmlStorage("");
            var repo = dal.GetRepository<TestObject>();
            var obj = new TestObject(); ;

            repo.Upsert(obj);
            dal.Save();
            
            Assert.True(repo.Get().Any(), "The object could not be added to the repository");
        }
    }
}