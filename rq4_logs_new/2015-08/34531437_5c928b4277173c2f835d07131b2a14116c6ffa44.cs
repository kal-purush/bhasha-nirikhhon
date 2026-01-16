using System;
using System.Collections.Generic;
using System.Linq;
using LazyStorage.Tests.StorageTypes;
using Xunit;

namespace LazyStorage.Tests
{
    public class PersistenceTests : IDisposable
    {
        public static IEnumerable<object[]> StorageTypes
        {
            get
            {
                return new[]
                {
                    new object[] {new InMemoryTestStorage(), },
                    new object[] {new XmlTestStorage()},
                };
            }
        }

        private ITestStorage currentStorage;

        [Theory, MemberData("StorageTypes")]
        public void CanSaveToStorage(ITestStorage storage)
        {
            currentStorage = storage;
            var dal = storage.GetStorage();
            var repo = dal.GetRepository<TestObject>();
            var obj = new TestObject(); ;

            repo.Upsert(obj);
            dal.Save();
            
            Assert.True(repo.Get().Any(), "The object could not be added to the repository");
        }

        [Theory, MemberData("StorageTypes")]
        public void StoragePersists(ITestStorage storage)
        {
            currentStorage = storage;
            var dal = storage.GetStorage();
            var repo = dal.GetRepository<TestObject>();
            var obj = new TestObject(); ;

            repo.Upsert(obj);
            dal.Save();

            var dal2 = storage.GetStorage();
            var repo2 = dal2.GetRepository<TestObject>();

            Assert.True(repo2.Get().Single().ContentEquals(obj), "The object could not be found in the persistent repo");
        }

        public void Dispose()
        {
            currentStorage.CleanUp();
        }
    }
}