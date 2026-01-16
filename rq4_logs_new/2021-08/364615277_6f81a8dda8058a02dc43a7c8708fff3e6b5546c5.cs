using App.Models.DTOs;
using App.Models.DTOs.CreateRequests;
using App.Models.DTOs.UpdateRquests;
using App.Models.Entities;
using App.Services;
using App.Services.Base;
using App.Services.Interface;
using MaxV.Base.DTOs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace App.Controllers.Base
{
    public interface ICRUDContoller<TEntity, TCreateRequest, TUpdateRequest, TViewModel, TKey>
        where TCreateRequest : BaseCreateRequest, new()
        where TUpdateRequest : BaseUpdateRequest<TKey>, new()
    {
        Task<ActionResult> Post([FromBody] TCreateRequest request);
        Task<ActionResult> Put(TKey id, TUpdateRequest request);
        Task<ActionResult> Delete(TKey id); 
        Task<ActionResult> GetById(TKey id);
        Task<ActionResult> GetAll();
    }

    public abstract class CRUDContoller<TEntity, TCreateRequest, TUpdateRequest, TViewModel, TKey> : ApiController, ICRUDContoller<TEntity, TCreateRequest, TUpdateRequest, TViewModel, TKey>
        where TEntity : class, new()
        where TCreateRequest : BaseCreateRequest, new()
        where TUpdateRequest : BaseUpdateRequest<TKey>, new()
        where TViewModel : BaseViewModel<TKey>, new()
    {

        private readonly IBaseService<TEntity, TCreateRequest, TUpdateRequest, TViewModel, TKey> _baseService;
        public CRUDContoller(ILogger logger, IBaseService<TEntity, TCreateRequest, TUpdateRequest, TViewModel, TKey> baseService) : base(logger)
        {
            _baseService = baseService;
        }
        [HttpPost]
        public virtual async Task<ActionResult> Post([FromBody] TCreateRequest request)
        {
            if (null == request)
                return BadRequest();
            var result = await _baseService.CreateAsync(request);

            if (null == result)
                return StatusCode(StatusCodes.Status500InternalServerError);
            return Ok(result);
        }
        [HttpPut("{id}")]
        public virtual async Task<ActionResult> Put(TKey id, TUpdateRequest request)
        {
            if (!id.Equals(request.Id))
                return BadRequest();
            var result = await _baseService.UpdateAsync(id, request);
            if (result > 0)
                return NoContent();
            return NotFound();
        }
        [HttpDelete("{id}")]
        public virtual async Task<ActionResult> Delete(TKey id)
{
            var result = await _baseService.DeleteHardAsync(id);
            if (result > 0)
                return Ok();
            return NotFound();
        }
        [HttpGet("")]
        public virtual async Task<ActionResult> GetAll()
{
            var result = await _baseService.GetAllDTOAsync();
            return Ok(result);
        }
        [HttpGet("{id}")]
        public virtual async Task<ActionResult> GetById(TKey id)
        {
            var result = await _baseService.GetByIdAsync(id);

            if (result != null)
                return Ok(result);
            return NotFound();
        }
    }
}