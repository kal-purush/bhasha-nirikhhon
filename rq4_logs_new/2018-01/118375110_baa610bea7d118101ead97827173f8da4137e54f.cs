using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CounterStats.UI.ViewModels;
using Moq;
using NUnit.Framework;
using CounterStats.Business.Interfaces;

namespace CounterStrats.UI.Test.ViewModels
{
    public class CurrentGameViewModelTests
    {
        private CurrentGameViewModel _subject;
        private Mock<ICurrentGameUpdater> _updater;

        [SetUp]
        public void Setup()
        {
            _updater = new Mock<ICurrentGameUpdater>();
            _updater.Setup(c => c.Start());
            
            _subject = new CurrentGameViewModel(_updater.Object);
        }

        [Test]
        public void Constructor_StartsUpdater()
        {
            _updater.Verify(c => c.Start(), Times.Once);
        }

        [Test]
        public void Constructor_SetsDefaultValueForPlayerName()
        {
            Assert.That(_subject.PlayerName, Is.EqualTo("Waiting to connect to a game")); 
        }
    }
}