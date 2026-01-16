/*
 *     Copyright 2017 Adam Burton (adz21c@gmail.com)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using EventStore.ClientAPI;
using Miles.Aggregates;
using System;

namespace Miles.EventStore
{
    public class DynamicAggregateBuilder<TAggregate, TState> : IAggregateBuilder<TAggregate> where TState : class, IAppliesEvent, new()
    {
        private readonly ISerializer<TAggregate> serializer;

        private int version = 0;
        private readonly TState state = new TState();

        public DynamicAggregateBuilder(ISerializer<TAggregate> serializer)
        {
            this.serializer = serializer;
        }

        public void AddEvent(RecordedEvent @event)
        {
            var eventObj = serializer.DeSerialize(@event);
            ((dynamic)state).ApplyEvent(eventObj);
            version = @event.EventNumber;
        }

        public TAggregate Build()
        {
            throw new NotImplementedException();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~AggregateBuilder() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}