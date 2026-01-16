using System;
using System.Collections.Generic;
using Discord.WebSocket;
using Moq;
using Xunit;
using ChronoBot.Common.Systems;
using System.IO;
using System.Threading;
using ChronoBot.Common.UserDatas;
using ChronoBot.Utilities.Tools.Deadlines;

namespace ChronoBot.Tests.Tools.Deadline
{
    public class CountdownTests
    {
        private readonly Countdown _countdown;
        private readonly DeadlineFileSystem _fileSystem;
        private const ulong DefaultGuildId = 234567891;

        public CountdownTests()
        {
            var mockClient = new Mock<DiscordSocketClient>(MockBehavior.Loose);
            _fileSystem = new DeadlineFileSystem(Path.Combine(Directory.GetCurrentDirectory(), "Test Files", GetType().Name));
            _countdown = new Countdown(mockClient.Object, _fileSystem, new List<DeadlineUserData>());
            if (!File.Exists(Path.Join(_fileSystem.PathToSaveFile, GetType().Name + DefaultGuildId + ".xml")))
                return;

            File.Delete(Path.Join(_fileSystem.PathToSaveFile, GetType().Name + DefaultGuildId + ".xml"));
            _fileSystem = new DeadlineFileSystem(Path.Combine(Directory.GetCurrentDirectory(), "Test Files", GetType().Name));
            _countdown = new Countdown(mockClient.Object, _fileSystem, new List<DeadlineUserData>());
        }

        [Fact]
        public void Countdown_SetCountdown_Success()
        {
            var tomorrow = DateTime.Now.AddDays(1);
            _countdown.SetDeadline("TestCountdown", tomorrow, DefaultGuildId,
            DefaultGuildId, "Test", 420);

            var users = (List<DeadlineUserData>)_fileSystem.Load();
            var user = users.Find(x => x.Id == "TestCountdown");

            Assert.True(user.Name == "Test");
            Assert.True(user.Deadline.Day == tomorrow.Day);
            Assert.True(user.Deadline.Hour == tomorrow.Hour);
            Assert.True(user.Deadline.Minute == tomorrow.Minute);
            Assert.True(user.GuildId == DefaultGuildId);
            Assert.True(user.ChannelId == DefaultGuildId);
            Assert.True(user.Id == "TestCountdown");
            Assert.True(user.UserId == 420);
        }

        [Fact]
        public void Countdown_CountingDown_Success()
        {
            var tomorrow = DateTime.Now.AddDays(1);
            var user = _countdown.SetDeadline("TestCountingDown",
                tomorrow,
                DefaultGuildId,
                DefaultGuildId,
                "CountingDown",
                69);

            Thread.Sleep(1000);

            Assert.True(user.Name == "CountingDown");
            Assert.True(user.Deadline.Day == tomorrow.Day);
            Assert.True(user.Deadline.Hour == tomorrow.Hour);
            Assert.True(user.Deadline.Minute == tomorrow.Minute);
            Assert.True(user.GuildId == DefaultGuildId);
            Assert.True(user.ChannelId == DefaultGuildId);
            Assert.True(user.Id.Contains("€"));
            Assert.True(user.UserId == 69);
        }

        [Fact]
        public void Countdown_CountedDown_Success()
        {
            _countdown.SetDeadline("TestCountedDown",
                DateTime.Now.AddSeconds(1),
                DefaultGuildId,
                DefaultGuildId,
                "CountedDown",
                69);

            Thread.Sleep(3000);

            var users = (List<DeadlineUserData>)_fileSystem.Load();
            var user = users.Find(x => x.Id == "TestCountedDown");

            Assert.Null(user);
        }
    }
}