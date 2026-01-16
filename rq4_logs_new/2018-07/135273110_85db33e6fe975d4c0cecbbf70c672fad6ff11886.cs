using System.Collections.Generic;
using System.Diagnostics;
using CAFU.Core.Data.Entity;
using CAFU.Flow.Data.Entity;
using CAFU.Flow.Domain.Model;
using CAFU.Flow.Utility;
using CAFU.Generics.Data.Entity;
using CAFU.Generics.Domain.Repository;
using ExtraUniRx;
using NUnit.Framework;
using UniRx;
using UnityModule;

namespace CAFU.Flow.Domain.UseCase
{
    public class MultipleFlowUseCaseTest
    {
        class MultipleFlowUseCaseSpy : MultipleFlowUseCase
        {
            public MultipleFlowUseCaseSpy()
            {
                this.Initialize();
            }

            public void LoadDummy(Subject<float> oscillator1, Subject<float> oscillator2)
            {
                this.ModelMap = new Dictionary<string, IList<FlowModel>>
                {
                    {
                        "sample1",
                        new List<FlowModel>
                        {
                            new FlowModel(0, 10f, 1f, 0f, 1, new WeightedArray<int>(new Dictionary<int, int> {{1, 1}}))
                        }
                    },
                    {
                        "sample2",
                        new List<FlowModel>
                        {
                            new FlowModel(0, 10f, 1f, 0f, 1, new WeightedArray<int>(new Dictionary<int, int> {{1, 1}}))
                        }
                    },
                };
                this.StopWatchMap = new Dictionary<string, IStopWatch>
                {
                    {"sample1", new StopWatch(oscillator1)},
                    {"sample2", new StopWatch(oscillator2)},
                };
                this.SubjectMap = new Dictionary<string, ISubject<int>>
                {
                    {"sample1", new Subject<int>()},
                    {"sample2", new Subject<int>()},
                };
            }
        }

        class DummyGenericRepository<TEntity> : IGenericRepository<TEntity>
            where TEntity : IGenericEntity
        {
            public TEntity InjectEntity { get; set; }

            public TEntity GetEntity()
            {
                return this.InjectEntity;
            }

            public IEnumerable<TEntity> GetEntities()
            {
                throw new System.NotImplementedException();
            }
        }

        [Test]
        public void StartStopPauseResumeTest()
        {
            var usecase = new MultipleFlowUseCaseSpy();
            var observer = new TestObserver<int>();
            var oscillator1 = new Subject<float>();
            var oscillator2 = new Subject<float>();
            usecase.LoadDummy(oscillator1, oscillator2);
            usecase.GetIdFlowAsObservable("sample1").Subscribe(observer);

            // init
            Assert.AreEqual(0, observer.OnNextCount);

            // start
            usecase.Start("sample1");
            Assert.AreEqual(0, observer.OnNextCount);

            // tick1
            oscillator1.OnNext(1f);
            Assert.AreEqual(1, observer.OnNextCount);
            Assert.AreEqual(1, observer.OnNextValues[0]);

            // tick2
            oscillator1.OnNext(1f);
            Assert.AreEqual(2, observer.OnNextCount);
            Assert.AreEqual(1, observer.OnNextValues[1]);

            // stop & tick
            usecase.Stop("sample1");
            oscillator1.OnNext(1f);
            oscillator1.OnNext(1f);
            oscillator1.OnNext(1f);
            Assert.AreEqual(2, observer.OnNextCount);

            // start
            usecase.Start("sample1");
            Assert.AreEqual(2, observer.OnNextCount);

            // pause
            usecase.Pause("sample1");
            oscillator1.OnNext(1f);
            oscillator1.OnNext(1f);
            oscillator1.OnNext(1f);
            Assert.AreEqual(2, observer.OnNextCount);

            // resume
            usecase.Resume("sample1");
            oscillator1.OnNext(9f); // 9sec
            Assert.AreEqual(3, observer.OnNextCount);
            Assert.AreEqual(1, observer.OnNextValues[2]);

            oscillator1.OnNext(2f); // 11sec
            Assert.AreEqual(3, observer.OnNextCount);
        }
    }
}