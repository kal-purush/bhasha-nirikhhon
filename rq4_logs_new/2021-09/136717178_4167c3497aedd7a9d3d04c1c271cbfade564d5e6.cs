using System;
using System.Net;
using backend.src.DataSources;
using Kadmium_sACN.SacnReceiver;
using Moq;
using Xunit;

namespace backend.test
{
	public class DeviceDataSourceTests
	{
		[Fact]
		public void When_TheDataSourceIsCreated_Then_ItShouldListenOnTheIPv4AnyAddress()
		{
			var receiver = Mock.Of<IMulticastSacnReceiver>();
			var factory = Mock.Of<IDmxWriterFactory>();
			var source = new DeviceDataSource(factory, receiver);

			Mock.Get(receiver)
				.Verify(x => x.Listen(IPAddress.Any));

		}
	}
}