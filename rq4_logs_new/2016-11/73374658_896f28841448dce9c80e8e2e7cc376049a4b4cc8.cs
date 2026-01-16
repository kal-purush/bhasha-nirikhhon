using FootballClient.DataAccess.Providers;
using FootballClient.Models;
using Prism.Commands;
using Prism.Windows.AppModel;
using Prism.Windows.Navigation;
using System.Collections.Generic;

namespace FootballClient.UWP.ViewModels
{
    public class CommentsPageViewModel : BaseViewModel
    {
        private readonly ISessionStateService _sessionStateService;
        private INavigationService _navigationService;
        private bool _isLoadingNow;
        private CommentType commentType = CommentType.News;
        private CommentsProvider _commentsProvider;

        public CommentsPageViewModel(ISessionStateService sessionStateService,
                                     INavigationService navigationService,
                                     CommentsProvider commentsProvider)
        {
            _commentsProvider = commentsProvider;
            _navigationService = navigationService;
            _sessionStateService = sessionStateService;
            RefreshCommand = new DelegateCommand(RefreshExecute, RefreshCanExecute);
            LoadNextPageCommand = new DelegateCommand(LoadNextPageExecute, LoadNextPageCanExecute);
            LoadPreviousPageCommand = new DelegateCommand(LoadPreviousPageExecute, LoadPreviousPageCanExecute);
        }

        private void RefreshExecute()
        {
            LoadData(CurrentNews.Id, CurrentIndex);
        }

        private bool RefreshCanExecute()
        {
            return !_isLoadingNow && CurrentNews != null;
        }

        private void LoadPreviousPageExecute()
        {
            LoadData(CurrentNews.Id, CurrentIndex - 1);
        }

        private bool LoadPreviousPageCanExecute()
        {
            return CurrentIndex > 0 && !_isLoadingNow;
        }

        private void LoadNextPageExecute()
        {
            LoadData(CurrentNews.Id, CurrentIndex + 1);
        }

        private bool LoadNextPageCanExecute()
        {
            return this.CurrentIndex < LastIndex && !this._isLoadingNow;
        }

        public DelegateCommand LoadNextPageCommand { get; private set; }
        public DelegateCommand LoadPreviousPageCommand { get; private set; }
        public DelegateCommand RefreshCommand { get; private set; }

        [RestorableState]
        public FeedItem CurrentNews { get; private set; }

        public bool IsResultEmpty { get; set; }

        public int LastIndex { get; set; }

        public int CurrentIndex { get; set; }

        public List<PageComment> Comments { get; private set; } = new List<PageComment>();

        public override void OnNavigatedTo(NavigatedToEventArgs e, Dictionary<string, object> viewModelState)
        {
            base.OnNavigatedTo(e, viewModelState);
            CurrentNews = _sessionStateService.SessionState["CurrentNews"] as FeedItem;
            LoadData(CurrentNews.Id, CurrentIndex, false);
        }

        private async void LoadData(uint postId, int pageIndex, bool isNeeedClear = true)
        {
            BusyCount++;
            _isLoadingNow = true;
            IsResultEmpty = false;
            LoadNextPageCommand.RaiseCanExecuteChanged();
            LoadPreviousPageCommand.RaiseCanExecuteChanged();
            RefreshCommand.RaiseCanExecuteChanged();

            var commentsResponse = await _commentsProvider.LoadComments(postId, pageIndex, commentType);
            if (commentsResponse != null)
            {
                CurrentIndex = commentsResponse.PagerCurrent;

                if (commentsResponse.PageComments != null)
                {
                    LastIndex = commentsResponse.TotalCount / 25;

                    Comments = commentsResponse.PageComments;
                }
            }

            _isLoadingNow = false;
            IsResultEmpty = Comments.Count == 0;
            LoadNextPageCommand.RaiseCanExecuteChanged();
            LoadPreviousPageCommand.RaiseCanExecuteChanged();
            RefreshCommand.RaiseCanExecuteChanged();

            BusyCount--;
        }
    }
}