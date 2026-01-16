using BrainyStories.Objects;
using Rg.Plugins.Popup.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace BrainyStories
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class QuizList : ContentPage
    {
        private Settings settingsPage;
        private QuizFactory factory = new QuizFactory();
        private StoryFactory factoryStories = new StoryFactory();

        public ObservableCollection<Quiz> ListOfQuizzes = new ObservableCollection<Quiz>();
        private ObservableCollection<Story> ListOfStories = new ObservableCollection<Story>();

        public QuizList()
        {
            ListOfStories = factoryStories.generateStories();
            for (int i = 0; i < ListOfStories.Count; i++)
            {
                ObservableCollection<Quiz> returnQuizzes = factory.GenerateQuizzes(ListOfStories.ElementAt(i).Name);
                foreach (Quiz quiz in returnQuizzes) 
                {
                    ListOfQuizzes.Add(quiz);
                }
            }
            InitializeComponent();
            BindList.ItemsSource = ListOfQuizzes;
            settingsPage = new Settings();
        }

        async void OnItemTapped(object sender, ItemTappedEventArgs e)
        {
            ListView view = (ListView)sender;
            if (view.SelectedItem == null)
            {
                return;
            }
            var quiz = (Quiz)view.SelectedItem;
            view.SelectedItem = null;
            await Navigation.PushAsync(new QuizPage(quiz));
        }

        // Navbar methods
        async void BackClicked(object sender, EventArgs e)
        {
            await App.Current.MainPage.Navigation.PopAsync();
        }

        async void HomeClicked(object sender, EventArgs e)
        {
            await App.Current.MainPage.Navigation.PopToRootAsync();
        }

        async void SettingsClicked(object sender, EventArgs e)
        {

            await PopupNavigation.Instance.PushAsync(settingsPage);
        }
    }
}