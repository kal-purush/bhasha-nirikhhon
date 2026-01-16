using Kittens.Views;
using Kittens.Services;
using Kittens.ViewModel;

namespace Kittens;

public static class MauiProgram
{
	public static MauiApp CreateMauiApp()
	{
		var builder = MauiApp.CreateBuilder();
		builder
			.UseMauiApp<App>()
			.ConfigureFonts(fonts =>
			{
				fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
				fonts.AddFont("OpenSans-Semibold.ttf", "OpenSansSemibold");
			});

        builder.Services.AddSingleton<GameService>();
        builder.Services.AddSingleton<GameViewModel>();
        builder.Services.AddSingleton<MainPage>();

        return builder.Build();
	}
}
