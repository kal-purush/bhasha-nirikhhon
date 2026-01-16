using CarsStorage.DAL.DbContexts;
using CarsStorage.DAL.Entities;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CarsStorage.DAL.Repositories.Implementations
{
	public class TechnicalWorksRepository(AppDbContext dbContext) : ITechnicalWorksRepository
	{
		/// <summary>
		/// Метод получения всего списка автомобилей.
		/// </summary>
		/// <returns>Список всех пользователей.</returns>
		public async Task<List<CarEntity>> GetList()
		{
			return await dbContext.Cars.ToListAsync();
		}


		/// <summary>
		/// Метод для создания объекта автомобиля.
		/// </summary>
		/// <param name="carEntity">Объект автомобиля.</param>
		/// <returns>Созданный объект автомобиля.</returns>
		public async Task<CarEntity> Create(CarEntity carEntity)
		{
			await dbContext.Cars.AddAsync(carEntity);
			await dbContext.SaveChangesAsync();
			return carEntity;
		}


		/// <summary>
		/// Метод для удаления объекта автомобиля по его идентификатору.
		/// </summary>
		/// <param name="id">Идентификатор объекта автомобиля.</param>
		public async Task Delete(int id)
		{
			var carEntity = await dbContext.Cars.FirstOrDefaultAsync(c => c.Id == id)
				?? throw new Exception("Автомобиль с заданным Id не найден");
			dbContext.Cars.Remove(carEntity);
			await dbContext.SaveChangesAsync();
		}
	}
}