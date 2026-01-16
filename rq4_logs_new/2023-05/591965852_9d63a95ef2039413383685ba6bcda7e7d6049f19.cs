using JobSeek.Api.Models.Entities;
using JobSeek.Api.Models.Input;
using JobSeek.Api.Services.Contracts;

namespace JobSeek.Api.Controllers
{
    public class AdministratorController : BaseController<IAdministratorService, Administrator, AdministratorInput>
    {
        public AdministratorController(IAdministratorService service) : base(service)
        { }
    }
}