using AutoMapper;
using HealthMate.API.ViewModels.Health;
using HealthMate.BLL.Abstractions;
using HealthMate.BLL.Models;
using Microsoft.AspNetCore.Mvc;

namespace HealthMate.API.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class HealthController
        (IModelWithNotesAndDateService<Health> modelWithNotesAndDateService, IMapper mapper)
        : ModelWithNotesAndDateController<Health, HealthViewModel, ShortHealthViewModel>(modelWithNotesAndDateService, mapper)
    {
    }
}