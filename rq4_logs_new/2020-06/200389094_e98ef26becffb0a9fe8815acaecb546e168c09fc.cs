using AutoMapper;
using CommonDomainObjects;
using Microsoft.AspNetCore.Mvc;
using Service;
using System.Threading.Tasks;

namespace Web.Controllers
{
    public abstract class DomainObjectController<TId, TDomainObject, TDomainObjectModel> : ControllerBase
        where TDomainObject : DomainObject<TId>
    {
        private readonly IDomainObjectService<TId, TDomainObject> _service;
        private readonly IMapper                                  _mapper;

        protected DomainObjectController(
            IDomainObjectService<TId, TDomainObject> service,
            IMapper                                  mapper
            )
        {
            _service = service;
            _mapper  = mapper;
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetAsync(
            TId id
            )
        {
            var domainObject = await _service.GetAsync(id);
            return domainObject == null ? (IActionResult)NotFound() : Ok(_mapper.Map<TDomainObjectModel>(domainObject));
        }
    }
}