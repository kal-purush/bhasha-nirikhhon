using System;
using System.Linq;
using CM.DTOs;
using CM.Web.Models;
using CM.Web.Providers.Contracts;

namespace CM.Web.Providers.ViewModelMappers
{
	public class BarViewMapper : IBarViewMapper
	{
		private readonly ICocktailViewMapper _cocktailViewMapper;
		private readonly ICommentViewMapper _commentViewMapper;
		public BarViewMapper(ICocktailViewMapper cocktailViewMapper, ICommentViewMapper commentViewMapper)
		{
			_commentViewMapper = commentViewMapper ?? throw new ArgumentNullException(nameof(commentViewMapper));
			_cocktailViewMapper = cocktailViewMapper ?? throw new ArgumentNullException(nameof(cocktailViewMapper));
		}
		public BarViewModel CreateBarViewModel(BarDTO barDTO)
		{
			return new BarViewModel
			{
				Id = barDTO.Id,
				Name = barDTO.Name,
				Country = barDTO.Address.CountryName,
				City = barDTO.Address.CityName,
				Street = barDTO.Address.Street,
				Phone = barDTO.Phone,
				Details = barDTO.Details,
				AverageRating = barDTO.AverageRating,
				ImagePath = barDTO.ImagePath,

				Comments = barDTO.Comments.Select(barDTO => this._commentViewMapper.CreateBarCommentViewModel(barDTO)).ToList(),

				Cocktails = barDTO.Cocktails.Select(cocktailDTO => this._cocktailViewMapper.CreateCocktailViewModel_Simple(cocktailDTO)).ToList()
			};
		}

		public BarIndexViewModel CreateBarIndexViewModel(BarDTO barDTO)
		{
			return new BarIndexViewModel
			{
				Id = barDTO.Id,
				Name = barDTO.Name,
				Country = barDTO.Address.CountryName,
				City = barDTO.Address.CityName,
				Street = barDTO.Address.Street,
				AverageRating = barDTO.AverageRating,
				ImagePath = barDTO.ImagePath,
				IsUnlisted = barDTO.IsUnlisted
			};
		}

		public BarDTO CreateBarDTO(BarViewModel barViewModel)
		{
			var addressDTO = CreateAddressDTO(barViewModel);

			var barDTO = new BarDTO
			{
				Id = barViewModel.Id,
				Name = barViewModel.Name,
				Phone = barViewModel.Phone,
				Details = barViewModel.Details,

				Address = addressDTO,

				Cocktails = barViewModel.SelectedCocktails.Select(sc => new CocktailDTO { Id = sc }).ToList()
			};

			return barDTO;
		}
		public BarDTO CreateBarDTO_simple(BarViewModel barViewModel)
		{
			return new BarDTO
			{
				Id = barViewModel.Id,
				Name = barViewModel.Name,
				Phone = barViewModel.Phone,
				Details = barViewModel.Details,
				ImagePath = barViewModel.ImagePath,
				Cocktails = barViewModel.SelectedCocktails.Select(sc => new CocktailDTO { Id = sc }).ToList()
			};
		}

		private AddressDTO CreateAddressDTO(BarViewModel barViewModel)
		{
			return new AddressDTO
			{
				CityId = barViewModel.CityID,
				Street = barViewModel.Street,
			};
		}
	}
}