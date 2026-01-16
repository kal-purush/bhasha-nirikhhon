using System.Collections.ObjectModel;
using TJC.MVVM.Models;
using TJC.MVVM.ViewModels;

namespace TJC.MVVM.Extensions.Conversion;

public static class ModelToViewModelConverter
{
    /// <summary>
    /// Create View Models from Models
    /// </summary>
    /// <typeparam name="TViewModel">View Model Type</typeparam>
    /// <typeparam name="TModel">Model Type</typeparam>
    /// <param name="models">Models</param>
    /// <returns>View Models</returns>
    public static ObservableCollection<TViewModel> CreateViewModelsFromModels<TViewModel, TModel>(IEnumerable<TModel> models)
        where TViewModel : ViewModelBase<TModel>
        where TModel : ModelBase
    {
        var viewModels = new ObservableCollection<TViewModel>();
        foreach (var model in models)
            if (Activator.CreateInstance(typeof(TViewModel), model) is TViewModel vm)
                viewModels.Add(vm);
        return viewModels;
    }
}