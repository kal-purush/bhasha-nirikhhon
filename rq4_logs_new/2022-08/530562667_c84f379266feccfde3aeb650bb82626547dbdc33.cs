using OpcCollector.Collector.SubscriberEvent;
using OpcCollector.Processor;
using OpcCollector.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Technosoftware.DaAeHdaClient.Da;

namespace OpcCollector.Collector
{
    public class DaCollector : IDisposable
    {
        public DaConnection Conn { get; }
        internal List<IProcessor> processors = new List<IProcessor>();
        private  bool _isRunning = false;
        private Dictionary<string, DaSubscriber> subs = new Dictionary<string, DaSubscriber>();

        public bool IsRunning => _isRunning;

        public DaCollector(DaConnection connection)
        {
            Conn = connection;
        }
        private void onDataHandler(OnData args)
        {
            foreach(var processor in processors)
            {
                processor.Apply(args);
            }
        }

        public void Register(IProcessor processor)
        {
            processors.Add(processor);
        }

        public void UnRegister(IProcessor processor)
        {
            processors.Remove(processor);
        }

        private DaSubscriber newSubscriber()
        {
            var sub = Conn.Subscribe(new TsCDaSubscriptionState { /*Name = $"OpcDaCollector#{GetHashCode()}", */Active = false, UpdateRate = 1000 });

            sub.OnData(this.onDataHandler);

            subs.Add(sub.sid, sub);

            return sub;
        }

        private void watchItems(List<TsCDaItem> items)
        {
            var sub = newSubscriber();
            sub.AddItems(items.ToArray());    
            Console.WriteLine("add chunk...");
        }

        public void Init()
        {
            if(Conn.IsClosed())
            {
                throw new DaConnectionClosedException();
            }

            var tags = new List<TagConfig>();

            foreach (var device in ConfigMgr.Instance.YmlConfig.Devices)
            {
                tags.AddRange(device.Tags);
            }

            var chunkItems = new List<TsCDaItem>(2);

            foreach (var tag in tags)
            {
                chunkItems.Add(new TsCDaItem { ItemName = tag.Name });

                if (chunkItems.Count == chunkItems.Capacity)
                {
                    watchItems(chunkItems);
                    chunkItems.Clear();
                }
            }

            if (chunkItems.Count > 0)
            {
                watchItems(chunkItems);
                //chunkItems.Clear();
            }
        }

        public void Run()
        {
            if(_isRunning)
            {
                return;
            }

            foreach(var subKV in subs)
            {
                subKV.Value.Resume();
            }

            _isRunning = true;
        }

        public void Stop()
        {
            if (!_isRunning)
            {
                return;
            }

            foreach (var subKV in subs)
            {
                subKV.Value.Pause();
            }

            _isRunning = false;
        }

        public void Dispose()
        {
            foreach(var subKV in subs)
            {
                //subKV.Value.Unsubscribe();
                subKV.Value.Dispose();
            }
        }
    }
}