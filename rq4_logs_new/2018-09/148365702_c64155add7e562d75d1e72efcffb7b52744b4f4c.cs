using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataBus.Configuration;
using MassTransit;

namespace DataBus
{
    public class Bus: IDisposable
    {
        private readonly string _url;
        private readonly string _userName;
        private readonly string _password;
        private IBusControl _busControl;

        public Bus()
            :this(null)
        {

        }

        public Bus(string connectionName)
        {
            var connection = connectionName == null
                ? Config.GetRabbitMqConfigSection().Connections[0]
                : Config.GetRabbitMqConfigSection().Connections[connectionName];

            _url = connection.Url;
            _userName = connection.UserName;
            _password = connection.Password;
            Initialize();
        }

        private void Initialize()
        {
            _busControl = MassTransit.Bus.Factory.CreateUsingRabbitMq(x =>
            {
                var host = x.Host(new Uri(_url), c =>
                {
                    c.Username(_userName);
                    c.Password(_password);
                });


            });
        }

        public Task Publish(object message)
        {
            return _busControl.Publish(message);
        }

        public Task Publish<TMessage>(TMessage message) where TMessage : class
        {
            return _busControl.Publish<TMessage>(message);
        }

        public void Start()
        {
            _busControl.Start();
        }

        public Task StartAsync()
        {
            return _busControl.StartAsync();
        }

        public void Stop()
        {
            _busControl.Stop();
        }

        public Task StopAsync()
        {
            return _busControl.StopAsync();
        }

        public void Dispose()
        {
            _busControl.Stop();
        }
    }
}