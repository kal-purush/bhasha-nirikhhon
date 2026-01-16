using System;
using FusionMVVM.Service;
using Xunit;

namespace FusionMVVM.Tests.Service
{
    public class EventAggregatorTests
    {
        [Fact]
        public void SubscribeWithoutTarget_ThrowsException()
        {
            var sut = new EventAggregator();
            Assert.Throws<ArgumentNullException>(() => sut.Subscribe<string>(null, s => { }));
        }

        [Fact]
        public void SubscribeWithoutAction_ThrowsException()
        {
            var sut = new EventAggregator();
            Assert.Throws<ArgumentNullException>(() => sut.Subscribe<string>(this, null));
        }

        [Fact]
        public void SubscribeMessageTypeNotFound()
        {
            var sut = new EventAggregator();
            var actual = sut.GetSubscribers<string>().Count;
            Assert.Equal(0, actual);
        }

        [Fact]
        public void SubscribeReturnsCorrectResult()
        {
            var sut = new EventAggregator();
            sut.Subscribe<string>(this, s => { });

            var actual = sut.GetSubscribers<string>().Count;

            Assert.Equal(1, actual);
        }
    }
}