using CarsInfo.BLL.Models.Dtos;
using CarsInfo.WebApi.ViewModels.ViewModels;

namespace CarsInfo.WebApi.Mappers
{
    public class CarsControllerMapper
    {
        public CarEditorViewModel MapToCarEditorViewModel(CarEditorDto carEditorDto)
        {
            if (carEditorDto is null)
            {
                return null;
            }

            return new CarEditorViewModel
            {
                BrandId = carEditorDto.BrandId,
                CarPicturesUrls = carEditorDto.CarPicturesUrls,
                Description = carEditorDto.Description,
                Model = carEditorDto.Model
            };
        }

        public CarEditorDto MapToCarEditorDto(CarEditorViewModel carEditorViewModel)
        {
            if (carEditorViewModel is null)
            {
                return null;
            }

            return new CarEditorDto
            {
                BrandId = carEditorViewModel.BrandId,
                CarPicturesUrls = carEditorViewModel.CarPicturesUrls,
                Description = carEditorViewModel.Description,
                Model = carEditorViewModel.Model
            };
        }
    }
}