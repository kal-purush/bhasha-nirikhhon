using AutoMapper;
using MealService.Application.DTOs.Meals;
using MealService.Application.Specifications;
using MediatR;

namespace MealService.Application.UseCases.Meals.Queries.GetFilteredMeals
{
    public class GetFilteredMealsHandler : IRequestHandler<GetFilteredMealsQuery, List<MealDto>>
    {
        private readonly IMapper _mapper;
        private readonly IUnitOfWork _unitOfWork;

        public GetFilteredMealsHandler(IMapper mapper, IUnitOfWork unitOfWork)
        {
            _mapper = mapper;
            _unitOfWork = unitOfWork;
        }
        public async Task<List<MealDto>> Handle(GetFilteredMealsQuery request, CancellationToken cancellationToken)
        {
            var filter=request.Filter;

            var meals = await _unitOfWork.MealRepository.GetPagedListAsync(
                request.PageNo,
                request.PageSize,
                cancellationToken,
                m => m.CuisineId == filter.CuisineId &&
                     (!filter.IsAvailable.HasValue || m.IsAvailable == filter.IsAvailable) &&
                     (!filter.CategoryId.HasValue || m.CategoryId == filter.CategoryId)
            );

            var mealDtos = _mapper.Map<List<MealDto>>(meals);

            return mealDtos;
        }
    }
}