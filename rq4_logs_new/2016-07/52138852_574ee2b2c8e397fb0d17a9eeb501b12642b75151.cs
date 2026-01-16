using Common.Logging;
using GladLive.Common;
using GladNet.Common;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GladLive.ProxyLoadBalancer.Tests
{
	[TestFixture]
	public static class GameServicePeerSessionTests
	{
		[Test]
		public static void Test_Ctor_Doesnt_Throw_On_Non_Null_Parameters()
		{
			Assert.DoesNotThrow(() => new GameServicePeerSession(Mock.Of<ILog>(), Mock.Of<INetworkMessageSender>(),
				Mock.Of<IConnectionDetails>(), Mock.Of<INetworkMessageSubscriptionService>(), Mock.Of<IDisconnectionServiceHandler>(), Mock.Of<IRequestPayloadHandlerService<GameServicePeerSession>>()));
		}
	}
}