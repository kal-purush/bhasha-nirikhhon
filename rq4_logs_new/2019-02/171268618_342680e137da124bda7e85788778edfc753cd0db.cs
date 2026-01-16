using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Timers;
using Nest;
using TaskZero.ReadModels.Elastic.Model;
using Timer = System.Timers.Timer;

namespace TaskZero.ReadModels.Elastic
{
    public class Indexer<T> : IIndexer<T> where T : class
    {
        private readonly int _interval;
        private readonly IElasticClient _elasticClient;
        private readonly string _indexName;
        private readonly List<T> _toBeAdded = new List<T>();
        private readonly List<string> _toBeDeleted = new List<string>();
        private Timer _timer;

        public Indexer(int interval, IElasticClient elasticClient, string indexName)
        {
            _interval = interval;
            _elasticClient = elasticClient;
            _indexName = indexName;
            Init();
        }

        private void Init()
        {
            if (!_elasticClient.IndexExists(_indexName).Exists)
            {
                _elasticClient.CreateIndex(_indexName, c => c
                    .InitializeUsing(GetIndexConfig())
                    .Mappings(m => m.Map<T>(mp => mp.AutoMap()))
                );
            }

            _timer = new Timer(_interval);
            _timer.Elapsed += Flush;
            _timer.AutoReset = true;
            _timer.Enabled = true;
            _timer.Start();
        }

        private IndexState GetIndexConfig()
        {
            var settings = new IndexSettings(); 

            var indexConfig = new IndexState
            {
                Settings = settings
            };
            return indexConfig;
        }

        public void Index(T doc)
        {
            _toBeAdded.Add(doc);
        }

        public void Remove(string id)
        {
            _toBeDeleted.Add(id);
        }

        private void Flush(object source, ElapsedEventArgs ea)
        {
            // TODO use a sync object to avoid concurrency instead of copy the list and clear...
            if (_toBeAdded.Any())
            {
                var docs = _toBeAdded.ToList();
                _toBeAdded.Clear();

                var waitHandle = new CountdownEvent(1);

                var bulkAll = _elasticClient.BulkAll(docs, b => b
                    .Index(_indexName) /* index */
                    .Type<ZeroTask>()
                    .BackOffRetries(2)
                    .BackOffTime("30s")
                    .RefreshOnCompleted(true)
                    .MaxDegreeOfParallelism(4)
                    .Size(1000)
                );

                bulkAll.Subscribe(new BulkAllObserver(
                    //onNext: (b) => { Console.Write("."); },
                    onError: (e) => throw e,
                    onCompleted: () => waitHandle.Signal()
                ));

                waitHandle.Wait();
            }

            if (_toBeDeleted.Any())
            {
                var toBeDeleted = _toBeDeleted.ToList();
                _toBeDeleted.Clear();
                foreach (var id in toBeDeleted)
                    _elasticClient.Delete<T>(id, d => d.Index(_indexName));                
            }
        }
    }
}