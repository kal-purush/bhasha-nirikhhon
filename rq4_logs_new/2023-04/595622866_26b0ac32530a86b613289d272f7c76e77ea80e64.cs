using Windows.ApplicationModel.Resources;
using Windows.UI;
using Windows.UI.ViewManagement;
using FileManager.Helpers;
using FileManager.Models;

namespace FileManager.Services
{
    public static class ThemeService
    {
        public static ColorMode ChangeColorMode(UISettings uiSettings, Color backgroundColor)
        {
            string resourcePath;
            ResourceLoader themeResourceLoader;
            ColorMode colorMode;
            var currentBackgroundColor = uiSettings?.GetColorValue(UIColorType.Background);
            if (currentBackgroundColor == Colors.Black)
            {
                resourcePath = string.Join('\\', Constants.Resources, Constants.ImagesDark);
                backgroundColor = Colors.Black;
            }
            else
            {
                resourcePath = string.Join('\\', Constants.Resources, Constants.ImagesLight);
                backgroundColor = Colors.White;
            }
            themeResourceLoader = ResourceLoader.GetForViewIndependentUse(resourcePath);
            colorMode = new ColorMode
            {
                BackgroundColor = backgroundColor,
                ThemeResourceLoader = themeResourceLoader
            };
            return colorMode;
        }
    }
}