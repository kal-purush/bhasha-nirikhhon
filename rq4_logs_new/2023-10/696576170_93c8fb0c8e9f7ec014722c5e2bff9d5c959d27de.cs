using MediatR;
using Microsoft.AspNetCore.Mvc;
using System.Net.Http;
using System.Threading.Tasks;
using Domain;
using Application.Commands;

namespace E_CommerceBackend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LoginController : ControllerBase
    {
        private readonly IHttpClientFactory _httpClientFactory;

        public LoginController(IHttpClientFactory httpClientFactory)
        {
            _httpClientFactory = httpClientFactory;
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] LoginRequest loginRequest)
        {
            // Verifica si los datos de nombre y contraseña son válidos
            if (string.IsNullOrEmpty(loginRequest.nombre) || string.IsNullOrEmpty(loginRequest.contraseña))
            {
                return BadRequest("Nombre y contraseña son obligatorios.");
            }

            // Envia los datos a otra API con la ruta /auth/login
            var client = _httpClientFactory.CreateClient();
            var requestUrl = "http://localhost:3000/api/auth/login"; 
            var response = await client.PostAsJsonAsync(requestUrl, loginRequest);

            if (response.IsSuccessStatusCode)
            {
                // Si la solicitud fue exitosa, puedes manejar la respuesta aquí si es necesario
                // Por ejemplo, puedes extraer datos del token de autenticación si la otra API devuelve un token
                var token = await response.Content.ReadAsStringAsync();
           

                return Ok(token);
            }
            else
            {
                // Si la solicitud a la otra API falla, puedes manejar el error aquí
                return StatusCode((int)response.StatusCode, "Error al intentar iniciar sesión.");
            }
        }
    }

    public class LoginRequest
    {
        public string nombre { get; set; }
        public string contraseña { get; set; }
    }
}