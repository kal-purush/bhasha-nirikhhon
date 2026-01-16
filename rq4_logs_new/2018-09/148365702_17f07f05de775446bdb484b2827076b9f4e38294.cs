//Copyright 2018 Damir Garipov
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace DataBusService
{
    public class DatabusSynchronizationContext<TMessage> : IDatabusSynchronizationContext where TMessage : class
    {
        private readonly AutoResetEvent _publishResetEvent;
        private readonly AutoResetEvent _messageHandleResetEvent;
        private Func<TMessage, Task> _messageHandler;

        public DatabusSynchronizationContext(AutoResetEvent publishResetEvent, AutoResetEvent messageHandleResetEvent)
        {
            _publishResetEvent = publishResetEvent;
            _messageHandleResetEvent = messageHandleResetEvent;
        }

        public Task MessageHandle(object message)
        {
            return _messageHandler((TMessage) message);
        }

        public void Wait(Func<TMessage, Task> messageHandler)
        {
            _messageHandler = messageHandler;
            _messageHandleResetEvent.Set();
            _publishResetEvent.WaitOne();
            Console.WriteLine("Message handled");
        }

        public void Wait(Func<object, Task> messageHandler)
        {
            Wait((Func<TMessage, Task>)messageHandler);
        }

        public void Dispose()
        {
            
        }
    }
}