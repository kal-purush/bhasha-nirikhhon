using Dunk.Tools.Monitoring.Extensions;
using Dunk.Tools.Monitoring.State;
using NUnit.Framework;

namespace Dunk.Tools.Monitoring.Test.Extensions
{
    [TestFixture]
    public class CircuitBreakerExtensionsTests
    {
        [Test]
        public void CircuitBreakerTryCloseMovesToClosedStateIfCurrentlyClosed()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Close();

            circuitBreaker.TryClose();

            Assert.IsTrue(circuitBreaker.IsClosed);
        }

        [Test]
        public void CircuitBreakerTryCloseMovesToClosedStateIfCurrentlyHalfOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.MoveToHalfOpenState();

            circuitBreaker.TryClose();

            Assert.IsTrue(circuitBreaker.IsClosed);
        }

        [Test]
        public void CircuitBreakerTryCloseMovesToHalfOpenStateIfCurrentlyOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Open();

            circuitBreaker.TryClose();

            Assert.IsTrue(circuitBreaker.IsHalfOpen);
        }

        [Test]
        public void CircuitBreakerTryCloseReturnsTrueIfStateIsCurrentlyClsoed()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Close();

            Assert.IsTrue(circuitBreaker.TryClose());
        }

        [Test]
        public void CircuitBreakerTryCloseReturnsTrueIfStateIsCurrentlyHalfOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.MoveToHalfOpenState();

            Assert.IsTrue(circuitBreaker.TryClose());
        }

        [Test]
        public void CircuitBreakerTryCloseReturnsFalseIfStateIsCurrentlyOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Open();

            Assert.IsFalse(circuitBreaker.TryClose());
        }

        [Test]
        public void CircuitBreakerTryOpenMovesToHalfOpenStateIfCurrentlyClosed()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Close();

            circuitBreaker.TryOpen();

            Assert.IsTrue(circuitBreaker.IsHalfOpen);
        }

        [Test]
        public void CircuitBreakerTryOpenMovesToOpenStateIfCurrentlyHalfOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.MoveToHalfOpenState();

            circuitBreaker.TryOpen();

            Assert.IsTrue(circuitBreaker.IsOpen);
        }

        [Test]
        public void CircuitBreakerTryOpenMovesToOpenStateIfCurrentlyOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Open();

            circuitBreaker.TryOpen();

            Assert.IsTrue(circuitBreaker.IsOpen);
        }

        [Test]
        public void CircuitBreakerTryOpenReturnsFalseIfStateIsCurrentlyClsoed()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Close();

            Assert.IsFalse(circuitBreaker.TryOpen());
        }

        [Test]
        public void CircuitBreakerTryOpenReturnsTrueIfStateIsCurrentlyHalfOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.MoveToHalfOpenState();

            Assert.IsTrue(circuitBreaker.TryOpen());
        }

        [Test]
        public void CircuitBreakerTryOpenReturnsTrueIfStateIsCurrentlyOpen()
        {
            var circuitBreaker = new CircuitBreaker(5, 100);

            circuitBreaker.Open();

            Assert.IsTrue(circuitBreaker.TryOpen());
        }
    }
}