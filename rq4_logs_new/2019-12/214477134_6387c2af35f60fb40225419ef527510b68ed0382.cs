using AutoMapper;
using CentralDeErros.Application.Interfaces.Base;
using CentralDeErros.Domain.Interfaces.Base;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CentralDeErros.Application.Services.Base
{
    public class AppServiceBase<TViewModel, TModel> : IAppServiceBase<TViewModel, TModel> 
        where TViewModel : class
        where TModel : class
    {
        protected readonly IServiceBase<TModel> _service;
        protected readonly IMapper _mapper;
        public AppServiceBase(IServiceBase<TModel> service, IMapper mapper)
        {
            _service = service;
            _mapper = mapper;
        }
        public IList<TViewModel> GetAll()
        {
            IList<TModel> modelList = _service.GetAll();
            return _mapper.Map<IList<TViewModel>>(modelList);
        }

        public async Task<TViewModel> Add(TViewModel viewModel)
        {
            TModel model = _mapper.Map<TModel>(viewModel);
            return _mapper.Map<TViewModel>(await _service.Add(model));
        }

        public IList<TViewModel> Find(Func<TModel, bool> predicate)
        {
            return _mapper.Map<IList<TViewModel>>(_service.Find(predicate));
        }
    }
}