using Armazenagem3L_API.Logger;
using Armazenagem3L_API.Models;
using Armazenagem3L_API.Services;
using Armazenagem3L_API.Util;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Armazenagem3L_API.Controllers {
    [Route("api/carga")]
    [ApiController]
    public class CargaController : ControllerBase {

        private readonly ILoggerManager _logger;
        private readonly CargaService _service;

        public CargaController(ILoggerManager logger, CargaService service) {
            _logger = logger;
            _service = service;
        }

        // GET api/carga/listagem?id=5&motorista=1
        [HttpGet("listagem")]
        public IActionResult Get(int id = 0, int motorista = 0)
        {
            _logger.LogDebug("[INFO] Recebendo requisicao (Controller): GET Carga id =>" + JsonSerializer.Serialize(id));

            CustomResponse result = new CustomResponse();

            if (id == 0 && motorista == 0)
            {
                result = _service.listagemCargas();
            }
            if (id > 0 && motorista == 0)
            {
                result = _service.cargaById(id);
            }
            if (id == 0 && motorista > 0)
            {
                result = _service.cargaByMotoristaId(motorista);
            }
            else if (id > 0 && motorista > 0)
            {
                result = _service.cargaByIdAndMotoristaId(id, motorista);
            }
            return StatusCode((int)result.StatusCode, result.Retorno);
        }

        // POST api/<ValuesController>
        [HttpPost]
        public IActionResult Post([FromBody] Carga value) {
            _logger.LogDebug("[INFO] Recebendo requisicao (Controller): POST Carga =>" + JsonSerializer.Serialize(value));
            CustomResponse response = _service.Add(value);

            return StatusCode((int)response.StatusCode, response.Retorno);
        }

        // POST api/aceitarCarga
        [HttpPost("aceitarCarga")]
        public IActionResult AceitarCarga([FromBody] MotoristaCarga value) {
            _logger.LogDebug("[INFO] Recebendo requisicao (Controller): POST AceitarCarga =>" + JsonSerializer.Serialize(value));
            CustomResponse response = _service.AceitarCarga(value);

            return StatusCode((int)response.StatusCode, response.Retorno);
        }

        // POST api/recusarCarga
        [HttpPost("recusarCarga")]
        public IActionResult RecusarCarga([FromBody] MotoristaCarga value) {
            _logger.LogDebug("[INFO] Recebendo requisicao (Controller): POST RecusarCarga =>" + JsonSerializer.Serialize(value));
            CustomResponse response = _service.RecusarCarga(value);

            return StatusCode((int)response.StatusCode, response.Retorno);
        }

        // GET api/carga/recusadas/id
        [HttpGet("recusadas/{id}")]
        public IActionResult GetRecusadas(int id) {
            _logger.LogDebug("[INFO] Recebendo requisicao (Controller): GetRecusadas =>" + JsonSerializer.Serialize(id));
            CustomResponse result = _service.cargasRecusadas(id);
            return StatusCode((int)result.StatusCode, result.Retorno);
        }

        // DELETE api/<ValuesController>/5
        [HttpDelete("{id}")]
        public IActionResult Delete(int id)
        {
            _logger.LogDebug("[INFO] Recebendo requisicao (Controller): DELETE Carga id =>" + JsonSerializer.Serialize(id));
            CustomResponse response = _service.DeletarCarga(id);
            return StatusCode((int)response.StatusCode, response.Retorno);
        }
    }
}