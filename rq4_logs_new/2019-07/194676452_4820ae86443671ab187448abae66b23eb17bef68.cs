using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using ScheduleSystem.DataSource.Abstractions.Contracts;
using ScheduleSystem.DataSource.Implementation.Repositories;

namespace ScheduleSystem.DataSource.Implementation.Extensions
{
	public static class ServiceCollectionExtensions
	{
		public static IServiceCollection AddDataSource(this IServiceCollection services, string connectionString)
		{
			if (String.IsNullOrWhiteSpace(connectionString))
			{
				throw new ArgumentException(nameof(connectionString));
			}
			
			services.AddDbContext<ScheduleDbContext>(options => options.UseNpgsql(connectionString));
		
			services.RegisterRepositories();

			return services;
		}

		private static void RegisterRepositories(this IServiceCollection services)
		{
			services.AddScoped<IUserRepository, UserRepository>()
					.AddScoped<IDisciplineRepository, DisciplineRepository>();
		}
	}
}