using CarsStorage.Abstractions.General;
using CarsStorage.Abstractions.ModelsDTO.Location;

namespace CarsStorage.Abstractions.BLL.Services
{
	/// <summary>
	/// Интерфейс для определения локации пользователя.
	/// </summary>
	public interface ILocationService
	{
		/// <summary>
		/// Метод для определения локации пользователя по координатам.
		/// </summary>
		/// <param name="coordinates">Объект координат пользователя.</param>
		/// <returns>Объект локации пользователя.</returns>
		public Task<ServiceResult<LocationDTO>> GetUserLocation(CoordinateDTO coordinates);
	}
}