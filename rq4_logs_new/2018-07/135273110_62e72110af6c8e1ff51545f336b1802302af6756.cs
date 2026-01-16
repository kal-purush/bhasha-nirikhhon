using System.Collections.Generic;
using System.Linq;
using CAFU.Core.Domain.Model;
using CAFU.Core.Domain.Translator;
using CAFU.Core.Domain.UseCase;
using CAFU.Flow.Utility;
using CAFU.Generics.Data.Entity;
using CAFU.Generics.Domain.Repository;

namespace CAFU.Flow.Domain.UseCase
{
    public class ModelMapUseCase<TModel, TEntity, TEntityList, TModelTranslator> : IModelMapUseCase<TModel>
        where TModel : IModel, IPrimaryId
        where TModelTranslator : IModelTranslator<TEntity, TModel>, new()
        where TEntity : IGenericEntity
        where TEntityList : ScriptableObjectGenericEntityList<TEntity>
    {
        public class Factory : DefaultUseCaseFactory<ModelMapUseCase<TModel, TEntity, TEntityList, TModelTranslator>>
        {
            protected override void Initialize(ModelMapUseCase<TModel, TEntity, TEntityList, TModelTranslator> instance)
            {
                base.Initialize(instance);
                instance.Initialize();
            }
        }

        public IDictionary<int, TModel> Id2ModelMap { get; private set; }
        private IGenericRepository<TEntityList> GenericRepository { get; set; }
        private TModelTranslator Translator { get; set; }

        public void Load()
        {
            this.Id2ModelMap = this.GenericRepository.GetEntity().List
                .Select(it => this.Translator.Translate(it))
                .ToDictionary(it => it.PrimaryId);
        }

        private void Initialize()
        {
            this.GenericRepository = new GenericRepository<TEntityList>.Factory().Create();
            this.Translator = new TModelTranslator();
        }
    }
}