using Microsoft.AspNetCore.Mvc;
using WebGeoInfrastructure.DTOs.Client;
using WebGeoInfrastructure.Helpers;
using WebGeoInfrastructure.Interfaces.Services;

namespace WebGeoAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ClientController : ControllerBase
    {

        private readonly IClientService _clientService;
        public ClientController(IClientService clientService)
        {
            _clientService = clientService;
        }

        [HttpGet]
        [Route("getAll")]
        public async Task<MessagingHelper<List<ClientListDTO>>> GetAll()
        {
            return await _clientService.GetAll();
        }

        [HttpPost]
        [Route("create")]
        public async Task<MessagingHelper> Create(ClientInsertDTO clientInsert)
        {
            return await _clientService.Insert(clientInsert);
        }
    }
}