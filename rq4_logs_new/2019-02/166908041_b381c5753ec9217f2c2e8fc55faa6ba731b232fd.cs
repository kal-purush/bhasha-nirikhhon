using MvvmCross.Navigation;
using MvvmCross.ViewModels;
using Recipes.Services;
using Recipes.ViewModels;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

[assembly: MvxRouting(typeof(SimpleNavigationFacade), @"mvx://task/\?id=(?<id>[A-Z0-9]{32})$")]
namespace Recipes.Services
{
    public class SimpleNavigationFacade: IMvxNavigationFacade
    {
        public Task<MvxViewModelRequest> BuildViewModelRequest(string url,
            IDictionary<string, string> currentParameters)
        {
            // you can load data from a database etc.
            // try not to do a lot of work here, as the user is waiting for the UI to do something ;)
            var viewModelType = currentParameters["id"] == Guid.Empty.ToString("N") ? typeof(LoginViewModel) : typeof(RecipesViewModel);

            return Task.FromResult(new MvxViewModelRequest(viewModelType, new MvxBundle(), null));
        }
    }
}