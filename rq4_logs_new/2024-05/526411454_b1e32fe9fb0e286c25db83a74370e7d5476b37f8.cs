using CommunityToolkit.Mvvm.ComponentModel;
using System.Windows;
using WpfEFCoreStudy.Services.Interfaces;

namespace WpfEFCoreStudy.Services;

public class DialogService : IDialogService
{

    public static IDialogService GetInstance { get; } = new DialogService();

    public void Show<TWindow, TViewModel>(TViewModel viewModel = null)
        where TWindow : Window, new()
        where TViewModel : ObservableObject, new()
    {
        TWindow w = new TWindow();
        TViewModel v = viewModel ?? new TViewModel();
        w.DataContext = v;

        w.Show();
    }

    public void ShowDialog<TWindow, TViewModel>(TViewModel viewModel = null)
        where TWindow : Window, new()
        where TViewModel : ObservableObject, new()
    {
        TWindow w = new TWindow();
        TViewModel v = viewModel ?? new TViewModel();
        w.DataContext = v;

        w.ShowDialog();
    }

}