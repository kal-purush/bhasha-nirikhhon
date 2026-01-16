using AutoMapper;
using HealthMate.API.ViewModels.Medication;
using HealthMate.BLL.Abstractions;
using HealthMate.BLL.Models;
using Microsoft.AspNetCore.Mvc;

namespace HealthMate.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MedicationController(IMedicationService medicationService, IMapper mapper)
        : GenericController<Medication, MedicationViewModel, ShortMedicationViewModel>(medicationService, mapper)
    {
        [HttpGet]
        public async Task<ICollection<MedicationViewModel>> GetByDate([FromQuery] DateOnly date,
            CancellationToken token)
        {
            var medicationCollection = await medicationService.GetByDate(date, token);

            return mapper.Map<ICollection<MedicationViewModel>>(medicationCollection);
        }
    }
}