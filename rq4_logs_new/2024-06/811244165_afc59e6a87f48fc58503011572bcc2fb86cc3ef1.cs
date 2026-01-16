using AutoMapper;
using HealthMate.API.Abstractions;
using HealthMate.BLL.Abstractions;
using Microsoft.AspNetCore.Mvc;

namespace HealthMate.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ModelWithNotesAndDateController<TModel, TViewModel, TShorViewModel>
        (IModelWithNotesAndDateService<TModel> modelWithNotesAndDateService, IMapper mapper)
        : GenericController<TModel, TViewModel, TShorViewModel>(modelWithNotesAndDateService, mapper),
            IModelWithNotesAndDateController<TViewModel, TShorViewModel>
        where TModel : BaseModelWithNotesAndDate
        where TViewModel : BaseViewModelWithNotesAndDate
        where TShorViewModel : class
    {
        [HttpGet(Name = "GetByDate")]
        public async Task<TViewModel> GetByDate([FromQuery] DateOnly date,
            CancellationToken token)
        {
            var model = await modelWithNotesAndDateService
                .GetByDate(date, token);

            return mapper.Map<TViewModel>(model);
        }

        [HttpGet]
        public async Task<ICollection<TViewModel>> GetBetweenTwoDates([FromQuery] DateOnly startDate,
            [FromQuery] DateOnly finishDate,
            CancellationToken token)
        {
            var models = await modelWithNotesAndDateService
                .GetBetweenTwoDates(startDate, finishDate, token);

            return mapper.Map<ICollection<TViewModel>>(models);
        }
    }
}