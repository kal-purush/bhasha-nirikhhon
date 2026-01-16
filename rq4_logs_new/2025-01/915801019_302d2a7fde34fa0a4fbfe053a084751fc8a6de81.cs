using System;
using System.Collections.Generic;
using System.Linq;
using FreeView.Services.Interfaces;
using FreeView.ViewModels.Interfaces;
using FreeView.ViewPresenter.Interfaces;
using FreeView.Views.Interfaces;

namespace FreeView.Services
{
    public class CanvasService : ICanvasService, IDisposable
    {
        private readonly IViewPresenter _viewPresenter;
        private bool _disposed;

        public List<IBaseView> ChildViews { get; } = new();

        public CanvasService(IViewPresenter viewPresenter)
        {
            _viewPresenter = viewPresenter;
        }
        
        public void Clear() => ChildViews.Clear();

        public void Hide<TViewModel>(TViewModel viewModel) where TViewModel : IBaseViewModel
        {
            SetVisibility<TViewModel>(false);
        }

        public void Show<TViewModel>(TViewModel viewModel) where TViewModel : IBaseViewModel
        {
            SetVisibility<TViewModel>(true);
        }

        public void Show<TViewModel, TNavigationArgs>(TViewModel viewModel, TNavigationArgs navigationArgs)
            where TViewModel : IBaseViewModel<TNavigationArgs>
        {
            SetVisibility<TViewModel, TNavigationArgs>(true, navigationArgs);
        }

        private void SetVisibility<TViewModel>(bool isVisible) where TViewModel : IBaseViewModel
        {
            var view = FindViewByViewModel<TViewModel>();
            if (view == null) return;

            view.ViewModel.Prepare();
            view.SetVisible(isVisible);
        }

        private void SetVisibility<TViewModel, TNavigationArgs>(bool isVisible, TNavigationArgs navigationArgs)
            where TViewModel : IBaseViewModel<TNavigationArgs>
        {
            var view = FindViewByViewModel<TViewModel>();
            if (view == null) return;

            if (view.ViewModel is IBaseViewModel<TNavigationArgs> navArgsViewModel)
            {
                navArgsViewModel.Prepare(navigationArgs);
            }

            view.SetVisible(isVisible);
        }

        private IBaseView FindViewByViewModel<TViewModel>()
        {
            return ChildViews.FirstOrDefault(v => v?.ViewModel?.GetType() == typeof(TViewModel));
        }

        private void InitView<TViewModel>() where TViewModel : IBaseViewModel
        {
            AddView(_viewPresenter.PresentView<TViewModel>());
        }

        private void InitView<TViewModel, TNavigationArgs>(TNavigationArgs navigationArgs, bool isVisible = true)
            where TViewModel : IBaseViewModel<TNavigationArgs>
        {
            var view = _viewPresenter.PresentView<TViewModel, TNavigationArgs>(navigationArgs);
            AddView(view);
            if (isVisible) view.SetVisible(true);
        }

        private void AddView(IBaseView view)
        {
            if (view == null) throw new ArgumentNullException(nameof(view));
            if (!ChildViews.Contains(view)) ChildViews.Add(view);
        }

        private void RemoveView<TViewModel>()
        {
            var view = FindViewByViewModel<TViewModel>();
            if (view != null) ChildViews.Remove(view);
        }

        public void Dispose()
        {
            if (_disposed) return;

            foreach (var view in ChildViews.OfType<IDisposable>())
            {
                view.Dispose();
            }

            ChildViews.Clear();
            _disposed = true;
        }
    }
}