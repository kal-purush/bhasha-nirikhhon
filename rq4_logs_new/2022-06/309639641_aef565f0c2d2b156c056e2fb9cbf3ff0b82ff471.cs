using Microsoft.Extensions.DependencyInjection;
using Unitee.Services;

namespace Unitee.Extensions;

public static class ServiceCollectionExtension
{
	public static void AddCustomServices(this IServiceCollection services)
	{
		services.AddSingleton<ResponseMaker>();
	}
}