using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ms_spa.Api.Contract.Cliente;
using ms_spa.Api.Domain.Services.Interfaces;
using ms_spa.Api.Exceptions;

namespace ms_spa.Api.Controllers
{
    [ApiController]
    [Route("cliente")]
    public class ClienteController(IClienteService ClienteService) : BaseController
    {
        private readonly IClienteService _ClienteService = ClienteService;

        [HttpPost]
        [Authorize]
        public async Task<IActionResult> Adicionar(ClienteRequestContract contrato)
        {
            try
            {
                return Created("", await _ClienteService.Adicionar(contrato, 0));
            }
            catch (BadRequestException ex)
            {
                return BadRequest(RetornarModelBadRequest(ex));
            }
            catch (Exception ex)
            {

                return Problem(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public async Task<IActionResult> ObterTodos()
        {
            try
            {
                return Ok(await _ClienteService.ObterTodos(0));
            }
            catch (Exception ex)
            {

                return Problem(ex.Message);
            }
        }

        [HttpGet]
        [Route("{id}")]
        [Authorize]
        public async Task<IActionResult> ObterPorId(int id)
        {
            try
            {
                return Ok(await _ClienteService.ObterPorId(id, 0));
            }
            catch (NotFoundException ex)
            {
                return NotFound(RetornarModelNotFound(ex));
            }
            catch (Exception ex)
            {

                return Problem(ex.Message);
            }
        }

        [HttpPut]
        [Route("{id}")]
        [Authorize]
        public async Task<IActionResult> Atualizar(int id, ClienteRequestContract contrato)
        {
            try
            {
                return Ok(await _ClienteService.Atualizar(id, contrato, 0));
            }
            catch (NotFoundException ex)
            {
                return NotFound(RetornarModelNotFound(ex));
            }
            catch (BadRequestException ex)
            {
                return BadRequest(RetornarModelBadRequest(ex));
            }
            catch (Exception ex)
            {

                return Problem(ex.Message);
            }
        }

        [HttpDelete]
        [Route("{id}")]
        [Authorize]
        public async Task<IActionResult> Deletar(int id)
        {
            try
            {
                await _ClienteService.Inativar(id, 0);
                return NoContent();
            }
            catch (NotFoundException ex)
            {
                return NotFound(RetornarModelNotFound(ex));
            }
            catch (Exception ex)
            {

                return Problem(ex.Message);
            }
        }

    }
}