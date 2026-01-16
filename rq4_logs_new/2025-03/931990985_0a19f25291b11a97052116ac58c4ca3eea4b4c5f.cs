using AutoMapper;
using MealService.Application.DTOs;
using MealService.Application.Exceptions;
using MealService.Application.Specifications;
using MealService.Application.Specifications.Services;
using MealService.Domain.Entities;
using MediatR;
using static System.Net.WebRequestMethods;

namespace MealService.Application.UseCases.Meals.Commands.CreateMeal
{
    public class CreateMealHandler : IRequestHandler<CreateMealCommand, MealDto>
    {
        private readonly IMapper _mapper;
        private readonly IUnitOfWork _unitOfWork;
        private readonly IImageService _imageService;

        public CreateMealHandler(IMapper mapper, IUnitOfWork unitOfWork,IImageService imageService)
        {
            _mapper = mapper;
            _unitOfWork = unitOfWork;
            _imageService = imageService;
        }

        public async Task<MealDto> Handle(CreateMealCommand request, CancellationToken cancellationToken)
        {
            var category=await _unitOfWork.CategoryRepository.GetByIdAsync(request.Meal.CategoryId,cancellationToken);
            if (category is null)
            {
                throw new BadRequestException("Foreign key constraint is violated.");
            }

            var newMeal = _mapper.Map<Meal>(request.Meal);
            newMeal.Id = Guid.NewGuid();

            if (request.Meal.ImageFile != null)
            {
                var imageUrl = await _imageService.UploadImageAsync(request.Meal.ImageFile);
                newMeal.ImageUrl = imageUrl;
            }
            else
            {
                newMeal.ImageUrl = _imageService.GetDefaultImageUrl();
            }

            await _unitOfWork.MealRepository.AddAsync(newMeal, cancellationToken);

            await _unitOfWork.SaveAllAsync(cancellationToken);

            var addedMeal = _mapper.Map<MealDto>(newMeal);

            return addedMeal;
        }
    }
}