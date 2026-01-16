using Microsoft.AspNetCore.Mvc.Rendering;

namespace SmoothNanners.Web.Extensions;

internal static class ViewContextExtensions
{
    public const string LocalPathPrefix = "~/";

    public static IReadOnlyList<string> GetScripts(this ViewContext viewContext)
    {
        return GetOrInitializeScripts(viewContext);
    }

    public static bool TryAddLocalScript(this ViewContext viewContext, string path)
    {
        if (!path.StartsWith(LocalPathPrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Script {nameof(path)} must be a local path starting with '~/'.",
                nameof(path));
        }

        var scripts = GetOrInitializeScripts(viewContext);
        if (!scripts.Contains(path))
        {
            scripts.Add(path);
            return true;
        }

        return false;
    }

    private static List<string> GetOrInitializeScripts(ViewContext viewContext)
    {
        const string scriptsKey = "ViewScripts";
        viewContext.HttpContext.Items.TryAdd(scriptsKey, new List<string>());
        return (viewContext.HttpContext.Items[scriptsKey] as List<string>)!;
    }
}