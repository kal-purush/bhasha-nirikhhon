using System;
using System.Collections.Generic;
using ChronoBot.Common.Systems;
using ChronoBot.Common.UserDatas;
using ChronoBot.Utilities.Tools.Deadlines;
using Discord.WebSocket;
using Moq;
using Xunit;

namespace ChronoBot.Tests.Tools.Deadlines
{
    public class DeadlineTests
    {
        private const ulong DefaultGuildId = 234567891;
        
        [Fact]
        public void Deadline_SetDeadline_Success()
        {
            var mockClient = new Mock<DiscordSocketClient>(MockBehavior.Loose);
            var deadline = new Utilities.Tools.Deadlines.Deadline(mockClient.Object,
                new DeadlineFileSystem(), new List<DeadlineUserData>());

            var tomorrow = DateTime.Now.AddDays(1);
            var user = deadline.SetDeadline("TestDeadline", tomorrow, DefaultGuildId, DefaultGuildId,
                "DeadlineUser", 9001);
            
            Assert.Null(user);
        }
    }
}