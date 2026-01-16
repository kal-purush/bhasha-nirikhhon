using System.Collections.ObjectModel;
using System.Windows.Input;
using DataLib.Sqlite;
using DataLib.Sqlite.Model;
using Xamarin.Forms;

namespace mobileClient.ViewModels
{
    public class NewTrainingViewModel : BaseViewModel
    {
        private int _calories;
        private string _name;
        private bool _isRepeated;
        private bool _canAdd;
        private bool _canDelete;
        private Training _deleted;

        public NewTrainingViewModel()
        {
            AllTrainings = new ObservableCollection<Training>(ProductContext.Trainings.GetItems());
        }

        public ObservableCollection<Training> AllTrainings { get; set; }
        
        public int Calories
        {
            get => _calories;
            set
            {
                SetProperty(ref _calories, value);
                CanAddCheck();
            }
        }

        public string Name
        {
            get => _name;
            set
            {
                SetProperty(ref _name, value);
                CanAddCheck();
            }
        }

        public bool IsRepeated
        {
            get => _isRepeated;
            set => SetProperty(ref _isRepeated, value);
        }

        public Training Deleted
        {
            get => _deleted;
            set
            {
                SetProperty(ref _deleted, value);
                CanDelete = value != null && value.Id > 0;
            }
        }

        public bool CanAdd { get => _canAdd; set => SetProperty(ref _canAdd, value); }
        public bool CanDelete { get => _canDelete; set => SetProperty(ref _canDelete, value); }

        private void CanAddCheck()
        {
            CanAdd = Calories > 0 && !string.IsNullOrEmpty(Name);
        }

        public ICommand AddTrainingCommand =>
            new Command(() =>
            {
                var np = new Training
                { Id = ProductContext.Trainings.NewId(), Calories = Calories, Name = Name, IsRepeated = IsRepeated};
                ProductContext.Trainings.SaveItem(np);
                AllTrainings.Add(np);
                Calories = 0;
                Name = null;
                CanAddCheck();
            });

        public ICommand DeleteTrainingCommand =>
            new Command(() =>
            {
                ProductContext.Products.DeleteItem(Deleted.Id);
                AllTrainings.Remove(Deleted);
                OnPropertyChanged(nameof(AllTrainings));
                Deleted = null;
                CanDelete = false;
            });
    }
}