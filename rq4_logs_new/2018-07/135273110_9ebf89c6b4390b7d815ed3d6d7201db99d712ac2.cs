using System;
using CAFU.Core.Presentation.Presenter;
using CAFU.Flow.Domain.UseCase;

namespace CAFU.Flow.Presentation.Presenter
{
    public interface IMultipleModelFlowPresenter : IPresenter
    {
        IMultipleModelFlowUseCase MultipleModelFlowUseCase { get; }
    }

    public static class IMultipleModelFlowPresenterExtension
    {
        public static void LoadMultipleFlow(this IMultipleModelFlowPresenter presenter)
        {
            presenter.MultipleModelFlowUseCase.Load();
        }

        public static void StartMultipleFlow(this IMultipleModelFlowPresenter presenter, string key)
        {
            presenter.MultipleModelFlowUseCase.Start(key);
        }

        public static void StopMultipleFlow(this IMultipleModelFlowPresenter presenter, string key)
        {
            presenter.MultipleModelFlowUseCase.Stop(key);
        }

        public static void PauseMultipleFlow(this IMultipleModelFlowPresenter presenter, string key)
        {
            presenter.MultipleModelFlowUseCase.Pause(key);
        }

        public static void ResumeMultipleFlow(this IMultipleModelFlowPresenter presenter, string key)
        {
            presenter.MultipleModelFlowUseCase.Resume(key);
        }

        public static IObservable<TModel> GetModelMultipleFlow<TModel>(this IMultipleModelFlowPresenter presenter, string key)
        {
            return presenter.MultipleModelFlowUseCase.GetModelFlowAsObservable<TModel>(key);
        }
    }
}