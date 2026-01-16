using Akavache.Sqlite3.Internal;
using FootballClient.DataAccess;
using FootballClient.DataAccess.Providers;
using FootballClient.Models;
using Prism.Commands;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Prism.Windows.Navigation;

namespace FootballClient.UWP.ViewModels
{
    public class MatchesPageViewModel : BaseViewModel
    {
        private readonly CultureInfo ruInfo = new CultureInfo("ru-RU");
        private CancellationTokenSource _cancellationToken = new CancellationTokenSource();

        private bool _isInitialized;
        public List<Championat> Championats { get; set; }
        private readonly MatchesProvider _matchesProvider;
        private readonly AsyncLock _loaderLock = new AsyncLock();

        public void OnChampionatsChanged()
        {

        }

        public MatchesPageViewModel(MatchesProvider matchesProvider)
        {
            _matchesProvider = matchesProvider;
            Championats = new List<Championat>();
        }

        public DelegateCommand RefreshCommand { get; private set; }

        public DelegateCommand<GameList> ViewDetailsCommand { get; private set; }

        public DateTimeOffset MatchDateTime { get; set; }

        public override void OnNavigatedTo(NavigatedToEventArgs e, Dictionary<string, object> viewModelState)
        {
            base.OnNavigatedTo(e, viewModelState);
            if (!_isInitialized)
            {
                _isInitialized = true;
                MatchDateTime = DateTimeOffset.Now;
            }
        }

        public void OnMatchDateTimeChanged()
        {
            if (IsLoadingNow)
            {
                _cancellationToken.Cancel();
            }

            LoadData();
        }

        public bool IsLoadingNow { get; private set; }

        public bool IsErrorOcurred { get; private set; }

        public object ErorrText { get; private set; }

        private async Task LoadData(bool isRefresh = false, RequestAccessMode mode = RequestAccessMode.Server)
        {
            BusyCount++;
            IsErrorOcurred = false;
            IsLoadingNow = true;

            if (!isRefresh)
            {
                Championats = null;
            }

            using (await _loaderLock.LockAsync())
            {
                using (_cancellationToken = new CancellationTokenSource())
                {
                    try
                    {
                        var currentDateTime = MatchDateTime;
                        var matchesResponse = await _matchesProvider.LoadMatchesAsync(currentDateTime.DateTime, mode, _cancellationToken.Token);
                        var realResponseDate = DateTimeOffset.Parse(matchesResponse.ViewDate);
                        //TODO:
                        Championats = null;

                        var groups = matchesResponse.GameList.GroupBy(x => x.ChampName).ToList();
                        var localCopy = new List<Championat>();

                        foreach (var group in groups)
                        {
                            var t = group.GroupBy(x => x.TourName).ToList();
                            foreach (var match in t)
                            {
                                var matchList = match.ToList();
                                foreach (var gameList in matchList)
                                {
                                    gameList.ItemIndex = matchList.IndexOf(gameList);
                                }

                                localCopy.Add(new Championat()
                                {
                                    Name = group.Key,
                                    TourName = match.Key,
                                    GameLists = matchList
                                });
                            }
                        }

                        Championats = new List<Championat>(localCopy);
                    }
                    catch (Exception ex) when(!(ex is OperationCanceledException))
                    {
                        //TODO:
                        //if (InternetTools.IsConnected)
                        {
                            ErorrText = "CommonResources.NoDataMessage";
                        }
                        //else
                        {
                            ErorrText = "CommonResources.CheckNetworkMessage";
                        }

                        IsErrorOcurred = true;
                    }
                }
            }

            _cancellationToken = new CancellationTokenSource();
            IsLoadingNow = false;
            BusyCount--;
        }
    }
}