using CarsStorage.DAL.Entities;

namespace CarsStorage.Abstractions.DAL.Repositories
{
	/// <summary>
	/// Интерфейс для репозитория состояний (конфигураций) БД.
	/// </summary>
	public interface IDbStatesRepository
	{
		/// <summary>
		/// Метод возвращает булево значение, проводятся ли технические работы в настоящий момент.
		/// </summary>
		/// <returns>Возвращает true, если проводятся технические работы.</returns>
		public Task<bool> IsUnderMaintenance();


		/// <summary>
		/// Метод для изменения состояния БД.
		/// </summary>
		/// <param name="dbStateEntity">Объект состояния БД.</param>
		public Task Update(bool value);
	}
}