using System.Security.Claims;
using Microsoft.AspNetCore.Mvc;
using ms_spa.Api.Contract;

namespace ms_spa.Api.Controllers
{
    public class BaseController : ControllerBase
    {
        protected int _idUsuario;
        protected int ObterIdUsuarioLogado()
        {
            var id = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

            _ = int.TryParse(id, out int idUsuario);

            return idUsuario;
        }

        protected static ModelErrorContract RetornarModelBadRequest(Exception ex)
        {
            return new ModelErrorContract
            {
                StatusCode = 400,
                Title = "Bad Request",
                Message = ex.Message,
                DateTime = DateTime.Now
            };
        }

        protected static ModelErrorContract RetornarModelNotFound(Exception ex)
        {
            return new ModelErrorContract
            {
                StatusCode = 404,
                Title = "Not Found",
                Message = ex.Message,
                DateTime = DateTime.Now
            };
        }

        protected static ModelErrorContract RetornarModelUnauthorized(Exception ex)
        {
            return new ModelErrorContract
            {
                StatusCode = 401,
                Title = "Unauthorized",
                Message = ex.Message,
                DateTime = DateTime.Now
            };
        }

    }
}