using ApiModuloFinal.Model;
using ApiModuloFinal.ViewModel;
using Microsoft.AspNetCore.Mvc;

namespace ApiModuloFinal.Controllers
{
    [ApiController]
    [Route("api/v1/cliente")]
    public class ClienteController : ControllerBase
    {
        private readonly IClienteRepository _clienteRepository;

        public ClienteController(IClienteRepository clienteRepository)
        {
            _clienteRepository = clienteRepository ?? throw new ArgumentNullException(nameof(clienteRepository));
        }

        [HttpPost]
        public IActionResult Add(ClienteViewModel clienteView)
        {
            var cliente = new Cliente(clienteView.Nome, clienteView.CPF, clienteView.email, clienteView.Telefone,
                                      clienteView.Logradouro, clienteView.Cidade, clienteView.CEP, clienteView.UF);

            _clienteRepository.Create(cliente);

            return Ok();
        }

        [HttpGet]
        public IActionResult GetAll()
        {
            var cliente = _clienteRepository.GetAll();

            return Ok(cliente);
        }

        [HttpGet("{id}")]
        public IActionResult GetByiD(int id)
        {
            var cliente = _clienteRepository.GetByID(id);
            return Ok(id);

        }

    }
}