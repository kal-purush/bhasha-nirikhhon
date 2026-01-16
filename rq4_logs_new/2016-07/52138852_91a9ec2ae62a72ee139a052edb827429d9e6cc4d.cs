using Common.Logging;
using GladLive.Common;
using GladNet.Common;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Moq.Protected;

namespace GladLive.ProxyLoadBalancer.Tests
{
	[TestFixture]
	public static class UserClientPeerSessionTests
	{
		[Test]
		public static void Test_Ctor_Doesnt_Throw_On_Non_Null_Parameters()
		{
			//These sorts of tests might seem stupid but it just caught two faults
			//I checked the wrong object for null.
			//assert
			Assert.DoesNotThrow(() => new UserClientPeerSession(Mock.Of<ILog>(), Mock.Of<INetworkMessageSender>(),
				Mock.Of<IConnectionDetails>(), Mock.Of<INetworkMessageSubscriptionService>(), Mock.Of<IDisconnectionServiceHandler>(), Mock.Of<IRequestPayloadHandlerService<UserClientPeerSession>>()));
		}

		[Test]
		public static void Test_Ctor_Throws_On_Null_Handler_Service()
		{
			//These sorts of tests might seem stupid but it just caught two faults
			//I checked the wrong object for null.
			//assert
			Assert.Throws<ArgumentNullException>(() => new UserClientPeerSession(Mock.Of<ILog>(), Mock.Of<INetworkMessageSender>(),
				Mock.Of<IConnectionDetails>(), Mock.Of<INetworkMessageSubscriptionService>(), Mock.Of<IDisconnectionServiceHandler>(), null));
		}

		[Test]
		public static void Test_Session_Doesnt_Process_Unencrypted_Messages()
		{
			//arrange
			Mock<IRequestPayloadHandlerService<UserClientPeerSession>> handler = new Mock<IRequestPayloadHandlerService<UserClientPeerSession>>(MockBehavior.Loose);
			UserClientPeerSession session = new UserClientPeerSession(Mock.Of<ILog>(), Mock.Of<INetworkMessageSender>(),
				new Mock<IConnectionDetails>(MockBehavior.Loose).Object, Mock.Of<INetworkMessageSubscriptionService>(), Mock.Of<IDisconnectionServiceHandler>(), handler.Object);
			Mock<IMessageParameters> messageParameters = new Mock<IMessageParameters>();
			messageParameters.SetupGet(mp => mp.Encrypted).Returns(false);

			//act
			//Generate and bind a method to a delegate
			Action<PacketPayload, IMessageParameters> method = GrabProtectedOnRecieveRequestMethod(session);
			method.Invoke(Mock.Of<PacketPayload>(), messageParameters.Object);

			//assert
			//Make sure the message didn't make it to the handler
			handler.Verify(x => x.TryProcessPayload(It.IsAny<PacketPayload>(), It.IsAny<IMessageParameters>(), session), Times.Never());
		}

		[Test]
		public static void Test_Session_Does_Process_Encrypted_Messages()
		{
			//arrange
			Mock<IRequestPayloadHandlerService<UserClientPeerSession>> handler = new Mock<IRequestPayloadHandlerService<UserClientPeerSession>>(MockBehavior.Loose);
			UserClientPeerSession session = new UserClientPeerSession(Mock.Of<ILog>(), Mock.Of<INetworkMessageSender>(),
				new Mock<IConnectionDetails>(MockBehavior.Loose).Object, Mock.Of<INetworkMessageSubscriptionService>(), Mock.Of<IDisconnectionServiceHandler>(), handler.Object);
			Mock<IMessageParameters> messageParameters = new Mock<IMessageParameters>();
			messageParameters.SetupGet(mp => mp.Encrypted).Returns(true);

			//act
			//Generate and bind a method to a delegate
			Action<PacketPayload, IMessageParameters> method = GrabProtectedOnRecieveRequestMethod(session);
			method.Invoke(Mock.Of<PacketPayload>(), messageParameters.Object);

			//assert
			//Make sure the message didn't make it to the handler
			handler.Verify(x => x.TryProcessPayload(It.IsAny<PacketPayload>(), It.IsAny<IMessageParameters>(), session), Times.Once());
		}

		[Test]
		public static void Test_Session_Throws_Null_Arg_OnRequestRecieve_When_Payload_Is_Null()
		{
			//arrange
			UserClientPeerSession session = new UserClientPeerSession(Mock.Of<ILog>(), Mock.Of<INetworkMessageSender>(),
				new Mock<IConnectionDetails>(MockBehavior.Loose).Object, Mock.Of<INetworkMessageSubscriptionService>(), Mock.Of<IDisconnectionServiceHandler>(), Mock.Of<IRequestPayloadHandlerService<UserClientPeerSession>>());

			//assert
			Assert.Throws<ArgumentNullException>(() => GrabProtectedOnRecieveRequestMethod(session).Invoke(null, Mock.Of<IMessageParameters>()));
		}

		[Test]
		public static void Test_Session_Throws_Null_Arg_OnRequestRecieve_When_MessageParameters_Is_Null()
		{
			//arrange
			UserClientPeerSession session = new UserClientPeerSession(Mock.Of<ILog>(), Mock.Of<INetworkMessageSender>(),
				new Mock<IConnectionDetails>(MockBehavior.Loose).Object, Mock.Of<INetworkMessageSubscriptionService>(), Mock.Of<IDisconnectionServiceHandler>(), Mock.Of<IRequestPayloadHandlerService<UserClientPeerSession>>());

			//assert
			Assert.Throws<ArgumentNullException>(() => GrabProtectedOnRecieveRequestMethod(session).Invoke(Mock.Of<PacketPayload>(), null));
		}

		public static Action<PacketPayload, IMessageParameters> GrabProtectedOnRecieveRequestMethod(UserClientPeerSession session)
		{
			return Delegate.CreateDelegate(typeof(Action<PacketPayload, IMessageParameters>), session, session.GetType().GetMethod("OnReceiveRequest", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public, null, new Type[] { typeof(PacketPayload), typeof(IMessageParameters) }, null)) as Action<PacketPayload, IMessageParameters>;
		}
	}
}