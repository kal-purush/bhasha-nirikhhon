using System.Collections.ObjectModel;
using System.Windows.Input;
using System.Threading.Tasks;
using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;

using AirporClientUWP.Models;
using AirporClientUWP.Services;

namespace AirporClientUWP.ViewModels
{
    public class AeroplaneTypeViewModel : ViewModelBase
    {
        private AeroplaneTypeService _service;
        private AeroplaneType _selectedAeroplaneType;

        public ObservableCollection<AeroplaneType> AeroplaneTypes { get; private set; }

        public AeroplaneTypeViewModel()
        {
            _service = new AeroplaneTypeService();
            DownloadData();

            AddCommand = new RelayCommand(AddAeroplaneType);
            UpdateCommand = new RelayCommand(UpdateAeroplaneType);
            DeleteCommand = new RelayCommand(DeleteAeroplaneType);
        }

        private async Task DownloadData()
        {
            try
            {
                AeroplaneTypes = await _service.GetAllAsync();
            }
            catch (System.InvalidOperationException)
            {
                throw;
            }
        }


        public AeroplaneType SelectedAeroplaneType
        {
            get { return _selectedAeroplaneType; }
            set
            {
                _selectedAeroplaneType = value;
                RaisePropertyChanged(() => SelectedAeroplaneType);
            }
        }

        public ICommand AddCommand { get; set; }

        private async void AddAeroplaneType()
        {
            try
            {
                var result = await _service.AddAsync(SelectedAeroplaneType);
                AeroplaneTypes.Insert(0, result);
            }
            catch (System.InvalidOperationException)
            {
                throw;
            }
        }

        public ICommand UpdateCommand { get; set; }

        private async void UpdateAeroplaneType()
        {
            try
            {
                var resultItem = await _service.UpdateAsync(SelectedAeroplaneType);
                AeroplaneTypes.Remove(SelectedAeroplaneType);
                AeroplaneTypes.Insert(0, resultItem);
            }
            catch (System.InvalidOperationException)
            {
                throw;
            }
        }

        public ICommand DeleteCommand { get; set; }

        private async void DeleteAeroplaneType()
        {
            try
            {
                await _service.DeleteAsync(SelectedAeroplaneType.Id);
                AeroplaneTypes.Remove(SelectedAeroplaneType);
            }
            catch (System.InvalidOperationException)
            {
                throw;
            }
        }
    }
}