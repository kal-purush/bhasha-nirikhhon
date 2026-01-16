using ChronoBot.Utilities.Tools;
using System;
using System.Collections.Generic;
using Discord.WebSocket;
using Moq;
using Xunit;
using ChronoBot.Common.Systems;
using System.IO;
using System.Threading;
using ChronoBot.Common.UserDatas;

namespace ChronoBot.Tests.Tools
{
    public class ReminderTests
    {
        private readonly Reminder _reminder;
        private readonly ReminderFileSystem _fileSystem;
        private const ulong DefaultGuildId = 234567891;

        public ReminderTests()
        {
            var mockClient = new Mock<DiscordSocketClient>(MockBehavior.Loose);
            _fileSystem = new ReminderFileSystem(Path.Combine(Directory.GetCurrentDirectory(), "Test Files", GetType().Name));
            _reminder = new Reminder(mockClient.Object, _fileSystem);
            if (!File.Exists(Path.Join(_fileSystem.PathToSaveFile, GetType().Name + DefaultGuildId + ".xml")))
                return;

            File.Delete(Path.Join(_fileSystem.PathToSaveFile, GetType().Name + DefaultGuildId + ".xml"));
            _fileSystem = new ReminderFileSystem(Path.Combine(Directory.GetCurrentDirectory(), "Test Files", GetType().Name));
            _reminder = new Reminder(mockClient.Object, _fileSystem);
        }

        [Fact]
        public void Reminder_SetReminder_Success()
        {
            var tomorrow = DateTime.Now.AddDays(1);
            var ok = _reminder.SetReminder("TestReminder", tomorrow, DefaultGuildId,
            DefaultGuildId, "Test");
            
            var users = (List<ReminderUserData>)_fileSystem.Load();
            var user = users.Find(x => x.Id == "TestReminder");

            Assert.True(user.Name == "Test");
            Assert.True(user.Deadline.Day == tomorrow.Day);
            Assert.True(user.Deadline.Hour == tomorrow.Hour);
            Assert.True(user.Deadline.Minute == tomorrow.Minute);
            Assert.True(user.GuildId == DefaultGuildId);
            Assert.True(user.ChannelId == DefaultGuildId);
            Assert.True(user.Id == "TestReminder");
            Assert.True(ok);
        }

        [Fact]
        public void Reminder_Reminded_Success()
        {
            var ok = _reminder.SetReminder("TestReminded", 
                DateTime.Now.AddSeconds(1), 
                DefaultGuildId,
                DefaultGuildId, 
                "Reminded");

            Thread.Sleep(3000);

            var users = (List<ReminderUserData>)_fileSystem.Load();
            var user = users.Find(x => x.Id == "TestReminded");

            Assert.Null(user);
            Assert.True(ok);
        }
    }
}