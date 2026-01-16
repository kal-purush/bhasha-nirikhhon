using System;
using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;
using GalaSoft.MvvmLight.Views;
using MyDriving.Core.Repositories;
using MyDriving.Models;

namespace MyDriving.ViewModels
{
    public class RefuelPageViewModel : ViewModelBase
    {
        private readonly INavigationService _navigationService;
        private readonly IRepository<Vehicle> _repository;

        public Vehicle Vehicle { get; set; }

        public int Liters { get; set; }
        public decimal Price { get; set; }
        public int Mileage { get; set; }

        public RelayCommand SaveCommand { get; }
        public RelayCommand CancelCommand { get; }

        public RefuelPageViewModel(INavigationService navigationService, IRepository<Vehicle> repository)
        {
            _navigationService = navigationService;
            _repository = repository;

            SaveCommand = new RelayCommand(OnSaveCommand);
            CancelCommand = new RelayCommand(OnCancelCommand);
        }

        private void OnCancelCommand()
        {
            _navigationService.GoBack();
        }

        private void OnSaveCommand()
        {
            Refuel refuel = MapToModel();
            Vehicle.Fuellings.Add(refuel);

            _repository.Update(Vehicle);
            _navigationService.GoBack();
        }

        private Refuel MapToModel()
        {
            return new Refuel
            {
                Liters = Liters,
                Mileage = Mileage,
                Price = Price,
                PricePerLiter = Math.Round(Price / Liters, 2)
            };
        }
    }
}