using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using DealCapture.Client.Annotations;
using DealCapture.Client.CreateDeal;
using DealCapture.Client.DealEnrichment;
using DealCapture.Client.Repositories;
using Microsoft.Practices.Prism.Commands;

namespace DealCapture.Client
{
    public class ActiveDealDashboardViewModel : INotifyPropertyChanged
    {
        private readonly IDealRepository _dealRepo;
        private readonly ObservableCollection<DealRowViewModel> _activeDeals = new ObservableCollection<DealRowViewModel>();
        private DealRowViewModel _selectedDeal;

        public ActiveDealDashboardViewModel(IDealRepository dealRepo)
        {
            _dealRepo = dealRepo;

            CreateDealCommand = new DelegateCommand(ShowDealCapture);
            EditSelectedDealCommand = new DelegateCommand(ShowDealEnrichment, ()=>SelectedDeal!=null);
        }

        public IDisposable Start()
        {
            //TODO: SOTW
            return _dealRepo.GetAllDealUpdates()
                .SubscribeOn(Scheduler.Default)
                .ObserveOnDispatcher()
                .Subscribe(ApplyRowUpdate);
        }

        public DelegateCommand CreateDealCommand { get; private set; }
        public DelegateCommand EditSelectedDealCommand { get; private set; }
        public DealRowViewModel SelectedDeal
        {
            get { return _selectedDeal; }
            set
            {
                if (Equals(value, _selectedDeal)) return;
                _selectedDeal = value;
                OnPropertyChanged();
                EditSelectedDealCommand.RaiseCanExecuteChanged();
            }
        }
        public ObservableCollection<DealRowViewModel> ActiveDeals { get { return _activeDeals; } }
        
        private void ShowDealCapture()
        {
            var dealId = Guid.NewGuid();

            var dealEntryVm = new DealEntryViewModel(_dealRepo, dealId);
            var view = new DealCaptureView
            {
                DataContext = dealEntryVm
            };

            _dealRepo.GetDealUpdates(dealId)
                .Take(1)
                .TakeUntil(Observable.FromEventPattern(h => view.Closed += h, h => view.Closed -= h))
                .ObserveOnDispatcher()
                .Subscribe(_ => view.Close());

            view.Show();
        }

        private void ShowDealEnrichment()
        {
            var dealEntryVm = new DealEnrichmentViewModel(_dealRepo, SelectedDeal.DealId);
            var view = new DealEnrichmentView
            {
                DataContext = dealEntryVm
            };

            view.Show();
        }

        private void ApplyRowUpdate(DealRowViewModel row)
        {
            int indexOf = IndexOf(ActiveDeals, ad => ad.DealId == row.DealId);  
            if (indexOf ==-1 )
            {
                ActiveDeals.Add(row);
            }
            else
            {
                ActiveDeals[indexOf] = row;
            }
        }

        private int IndexOf<T>(ObservableCollection<T> source, Predicate<T> filter)
        {
            for (int i = 0; i < source.Count; i++)
            {
                if (filter(source[i]))
                {
                    return i;
                }
            }
            return -1;
        }

        #region INPC

        public event PropertyChangedEventHandler PropertyChanged;

        [NotifyPropertyChangedInvocator]
        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChangedEventHandler handler = PropertyChanged;
            if (handler != null) handler(this, new PropertyChangedEventArgs(propertyName));
        }

        #endregion

    }
}