using NUnit.Framework;

namespace JetBrains.Util.Tests
{
  [TestFixture]
  public class LocalList2Tests
  {
    [Test]
    public void DefaultConstructor()
    {
      var list = new LocalList2<int>();

      Assert.AreEqua(list.);
      Assert.AreEqual(list.Count, 0);
      Assert.AreEqual(list.Capacity, 0);
    }

    [Test]
    public void CapacityConstructor()
    {
      foreach (var forceArray in new[] { true, false })
      for (var count = 0; count < 30; count++)
      {
        var list = new LocalList2<int>(capacity: 0, forceArray);

        Assert.AreEqual(list.Count, 0);
        Assert.AreEqual(list.Capacity, 0);
      }
    }
  }
}