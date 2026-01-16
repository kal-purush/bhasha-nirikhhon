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
using System.Threading.Tasks;
using MassTransit;

namespace DataBusService
{
    public class ConsumeObserver : IConsumeObserver
    {
        Task IConsumeObserver.PreConsume<T>(ConsumeContext<T> context)
        {
            // called before the consumer's Consume method is called
            return Console.Out.WriteLineAsync($"Received message {typeof(T)} from {context.SourceAddress}");
        }

        Task IConsumeObserver.PostConsume<T>(ConsumeContext<T> context)
        {
            // called after the consumer's Consume method is called
            // if an exception was thrown, the ConsumeFault method is called instead
            return Task.CompletedTask;
        }

        Task IConsumeObserver.ConsumeFault<T>(ConsumeContext<T> context, Exception exception)
        {
            // called if the consumer's Consume method throws an exception
//            await Console.Out.WriteLineAsync($"Error by handle message {typeof(T)}. Exception: {exception}");
            return Task.CompletedTask;
        }
    }
}