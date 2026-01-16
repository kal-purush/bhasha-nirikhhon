using MvvmApp.Core.Infrastructure.Application;
using MvvmApp.Core.Infrastructure.Messages;

namespace MvvmApp.Core.Features.MainPage;

public interface IMainPageNavigationService
{
    void OnNavigationMessageReceived(MainPageViewModel vm, ChangeMainPageMessage message);
}

public class MainPageNavigationService(
    IDispatcher dispatcher,
    IPageViewModelGetterService pageViewModelGetterService) : IMainPageNavigationService
{
    public async void OnNavigationMessageReceived(MainPageViewModel vm, ChangeMainPageMessage message)
    {
        var destination = pageViewModelGetterService.GetPageViewModel(message.Destination);
        await dispatcher.RunAsync(() => vm.SelectedView = destination);
    }
}