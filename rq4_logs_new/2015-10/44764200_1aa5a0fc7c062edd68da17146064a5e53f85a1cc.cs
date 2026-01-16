using System;
using System.Collections.Generic;
using System.Linq;
using SimpleCQRS.Domain;
using SimpleCQRS.Test.Eventing;
using SimpleCQRS.Test.Eventing.EventConstraintBaseTwo;
using SimpleCQRS.Test.Eventing.EventConstraintOne;
using Xunit;

namespace SimpleCQRS.Test
{
    public class AggregateRootRegistrationMock : AggregateRoot
    {
        private readonly List<object> _eventsHandled;
        public override Guid Id { get; }

        public AggregateRootRegistrationMock()
        {
            _eventsHandled = new List<object>();
        }

        public TEventType GetEvent<TEventType>()
        {
            return (TEventType)_eventsHandled.First(e => e is TEventType);
        }

        public void OnEvent1(Event1 @event)
        {
            _eventsHandled.Add(@event);
        }

        public void OnEvent2<TEventConstraintBase1>(
            Event2<TEventConstraintBase1> @event)
            where TEventConstraintBase1 : EventConstraintBase1
        {
            _eventsHandled.Add(@event);
        }

        public void OnEvent3<TEventConstraintBase1, TEventConstraintBase2>(
            Event3<TEventConstraintBase1, TEventConstraintBase2> @event)
            where TEventConstraintBase1 : EventConstraintBase1
            where TEventConstraintBase2 : EventConstraintBase2
        {
            _eventsHandled.Add(@event);
        }

        protected override void RegisterConflictResolvers()
        {
            throw new NotImplementedException();
        }

        public void CallRegisterEvents()
        {
            RegisterEvents();
        }

        public void HandleEvent<TEvent>(TEvent @event)
            where TEvent : Event
        {
            ApplyChange(@event);
        }

        protected override void RegisterEvents()
        {
            RegisterEvent<Event1>(OnEvent1);
            RegisterGenericEvents<EventConstraintBase1>(typeof(Event2<>));
            RegisterGenericEvents<EventConstraintBase1, EventConstraintBase2>(typeof(Event3<,>));
        }
    }

    public class AggregateRootTests
    {
        [Fact]
        public void RegisterGenericEventsWithAggregateRoot()
        {
            var mockRoot = new AggregateRootRegistrationMock();

            mockRoot.CallRegisterEvents();
        }

        [Fact]
        public void RegisterGenericEventsAndHandleSomeEvent()
        {
            var mockRoot = new AggregateRootRegistrationMock();
            mockRoot.CallRegisterEvents();
            var expected = new Event3<EventConstraint1, EventConstraintAnother2>();

            mockRoot.HandleEvent(expected);

            var actual = mockRoot.GetEvent<Event3<EventConstraint1, EventConstraintAnother2>>();

            Assert.Equal(expected, actual);
        }
    }
}