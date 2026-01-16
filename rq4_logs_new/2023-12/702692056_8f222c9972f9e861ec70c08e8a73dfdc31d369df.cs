namespace MentalHospital.BLL.Services
{
    internal class Service<TModel, TEntity> : IService<TModel> where TModel : class where TEntity : class
    {
        private readonly IRepository<TEntity> _repository;
        private readonly IMapper _mapper;

        public Service(IRepository<TEntity> repository, IMapper maper)
        {
            _repository = repository;
            _mapper = maper;
        }


        public async Task<TModel> Create(TModel model)
        {
            var entity = await _repository.Create(_mapper.Map<TEntity>(model));
            return _mapper.Map<TModel>(entity);
        }

        public async Task<TModel> Delete(Guid id)
        {
            var entity = await _repository.Delete(id);
            return _mapper.Map<TModel>(entity);
        }

        public async Task<TModel> Get(Guid id)
        {
            var entity = await _repository.Get(id);
            return _mapper.Map<TModel>(entity);
        }

        public async Task<IEnumerable<TModel>> GetAll()
        {
            var entities = await _repository.GetAll();
            return _mapper.Map<IEnumerable<TModel>>(entities);
        }

        public async Task<TModel> Update(TModel model)
        {
            var entity = await _repository.Update(_mapper.Map<TEntity>(model));
            return _mapper.Map<TModel>(entity);
        }
    }
}