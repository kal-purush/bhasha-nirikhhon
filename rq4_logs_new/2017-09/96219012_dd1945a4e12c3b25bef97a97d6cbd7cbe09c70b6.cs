using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Xunit;

namespace ActivityContext.Tests
{
    public class ActivityFailedExceptionTests
    {
        [Fact]
        public void ActivityFailedExceptionIsSerializable()
        {
            var ac = new Activity("Test");
            var ex = new ActivityFailedException(ac);

            var mem = new MemoryStream();
            var bf = new BinaryFormatter();

            bf.Serialize(mem, ex);
            mem.Position = 0;

            var ex2 = (ActivityFailedException)bf.Deserialize(mem);

            Assert.Equal(ex.Activity.Id, ex2.Activity.Id);
            Assert.Equal(ex.Activity.Name, ex2.Activity.Name);
        }
    }
}