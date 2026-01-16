using System;
using System.Collections.Generic;
using System.Linq;
using CAFU.Core.Domain.Model;
using CAFU.Core.Domain.UseCase;
using UniRx;

namespace CAFU.Flow.Domain.UseCase
{
    public class MultipleModelFlowUseCase :
        MultipleFlowUseCase,
        IMultipleModelFlowUseCase
    {
        public new class Factory : DefaultUseCaseFactory<MultipleModelFlowUseCase>
        {
            protected override void Initialize(MultipleModelFlowUseCase instance)
            {
                base.Initialize(instance);
                instance.Initialize();
            }
        }

        public IDictionary<string, IDictionary<int, IModel>> Key2Id2ModelMap { get; set; }

        protected override void Initialize()
        {
            base.Initialize();
            this.Key2Id2ModelMap = new Dictionary<string, IDictionary<int, IModel>>();
        }

        public void LoadModel<TModel>(IDictionary<int, TModel> modelMap)
        {
            var typeKey = typeof(TModel).FullName;
            this.Key2Id2ModelMap[typeKey] = modelMap.ToList().ToDictionary(it => it.Key, it => it.Value as IModel);
        }

        public IObservable<TModel> GetModelFlowAsObservable<TModel>(string key)
        {
            var typeKey = typeof(TModel).FullName;
            return base.GetIdFlowAsObservable(key).Select(id => this.Key2Id2ModelMap[typeKey][id]).Cast<IModel, TModel>();
        }
    }
}