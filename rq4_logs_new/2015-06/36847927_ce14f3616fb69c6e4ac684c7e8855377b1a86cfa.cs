using System.Data;
using Moq;
using NUnit.Framework;

namespace ExpressiveData.Tests
{
	[TestFixture]
	public class DataReaderWrapperTests
	{
		private const string PropertyWithAttribute = "ColumnA";
		private const string PropertyWithOutAttribute = "PropertyWithOutAttribute";
		public class TestClass
		{
			[Db(ColumnName = DataReaderWrapperTests.PropertyWithAttribute)]
			public string PropertyWithAttribute { get; set; }

			public string PropertyWithOutAttribute { get; set; }
		}

		[Test]
		public void OrdinalCachesValues()
		{
			var mockReader = new Moq.Mock<IDataReader>();
			mockReader.Setup(reader => reader.GetOrdinal(PropertyWithAttribute)).Returns(1);
			mockReader.Setup(reader => reader.GetOrdinal(PropertyWithOutAttribute)).Returns(2);

			var fakeReader = mockReader.Object;
			var wrapper = new DataReaderWrapper(fakeReader, new ExpressionMetaDataProvider());

			wrapper.GetOrdinal(PropertyWithAttribute);
			wrapper.GetOrdinal(PropertyWithAttribute);

			wrapper.GetOrdinal(PropertyWithOutAttribute);
			wrapper.GetOrdinal(PropertyWithOutAttribute);

			mockReader.Verify(reader => reader.GetOrdinal(PropertyWithAttribute), Times.AtMostOnce());
			mockReader.Verify(reader => reader.GetOrdinal(PropertyWithOutAttribute), Times.AtMostOnce());
			Assert.Pass("Ordinal only called once per value");
		}
	}
}