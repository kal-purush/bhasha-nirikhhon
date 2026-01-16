using System.ComponentModel;
using Avalonia.Collections;
using Frontend.Views;
using MaterialDesign.Avalonia.PackIcon;
using ReactiveUI;

namespace Frontend.ViewModels;

public class LogInViewModel : ReactiveObject, IRoutableViewModel
{
    public IScreen HostScreen { get; } = null!;
    public string? UrlPathSegment { get; } = "LogIn";
    public static LogIn _LogIn = new LogIn();
    private string _test;
    public string userName { get; set; } = "";
    public string passWord { get; set; } = "";
    
    public string Test
    {
        get => _test;
        set => _test = "Test1";
    }
    public AvaloniaList<TabItemViewModel> TabItems { get; set; } = new();

    public LogInViewModel()
    {
        TabItems.Add(new TabItemViewModel("Dashboard", new DashboardViewModel(), PackIconKind.ViewDashboard));
        TabItems.Add(new TabItemViewModel("Profile", new ProfileViewModel(), PackIconKind.Settings));
    }
}