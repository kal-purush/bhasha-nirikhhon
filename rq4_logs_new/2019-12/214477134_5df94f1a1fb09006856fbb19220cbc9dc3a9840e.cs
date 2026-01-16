using CentralDeErros.Api.Controllers.Base;
using CentralDeErros.Application.Interfaces;
using CentralDeErros.Application.ViewModel;
using Microsoft.AspNetCore.Mvc;
using System;

namespace CentralDeErros.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ErrorLogController : BaseController
    {
        private readonly IErrorLogAppService _errorLogAppService;
        public ErrorLogController(IErrorLogAppService errorLogAppService, IUserAppService userAppService) : base(userAppService)
        {
            _errorLogAppService = errorLogAppService;
        }
        // GET: api/ErrorLog
        [HttpGet]
        public ActionResult<ErrorLogViewModel> Get()
        {
            return Ok(_errorLogAppService.GetAll());
        }

        // POST: api/ErrorLog
        [HttpPost]
        public ActionResult<ErrorLogViewModel> Post([FromBody] ErrorLogViewModel errorLogViewModel)
        {
            return Ok(_errorLogAppService.Add(errorLogViewModel));
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public ActionResult<ErrorLogViewModel> Delete(Guid id)
        {
            var log = _errorLogAppService.Find(e => e.Id == id);
            if (log.Count > 0)
            {
                _errorLogAppService.DeleteErrorLog(id);
                return Ok("Log de erro deletado com sucesso");
            }
            return NotFound("Log de erro não encontrado para o Id " + id);
        }
    }
}