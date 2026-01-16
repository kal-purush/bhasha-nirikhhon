using AutoMapper;
using RMS.Application.Common.Interfaces;
using RMS.Application.Common.Interfaces.Repositories;
using RMS.Domain.Abstract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RMS.Application.Services
{
    public abstract class BaseService<TEntity, TBaseRequestEntity, TBaseResponseEntity> : IBaseService<TEntity, TBaseRequestEntity, TBaseResponseEntity> 
        where TEntity : EntityBase
    {
        private readonly IBaseRepository<TEntity> _repository;
        private readonly IMapper _mapper;

        public BaseService(IMapper mapper, IBaseRepository<TEntity> repository)
        {
            _mapper = mapper;
            _repository = repository;
        }
        public void Create(TBaseRequestEntity request)
        {
            var mappedEntity = _mapper.Map<TBaseRequestEntity, TEntity>(request);
            _repository.Create(mappedEntity);
        }

        public bool Delete(int id)
        {
            throw new NotImplementedException();
        }

        public IQueryable<TEntity> GetAll(int pageList, int pageNumber, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public TEntity GetById(int id)
        {
            var response = _repository.GetById(id);
            var test = _mapper.Map<TBaseResponseEntity>(response);
            return test;
        }

        public TBaseRequestEntity Update(int id, TBaseRequestEntity requset)
        {
            throw new NotImplementedException();
        }
    }
}