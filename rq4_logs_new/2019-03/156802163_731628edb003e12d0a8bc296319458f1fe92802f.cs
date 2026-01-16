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
	public partial class EndOfStory : ContentPage
	{
        private Settings settingsPage;

        public ObservableCollection<ThinkAndDo> ListOfThinkAndDos;
        public ObservableCollection<Quiz> ListOfQuizzes;

        public EndOfStory (Story story)
		{
            ListOfThinkAndDos = story.ThinkAndDos;
            ListOfQuizzes = story.Quizzes;
            InitializeComponent ();
            BindThinkAndDoList.ItemsSource = ListOfThinkAndDos;
            BindQuizList.ItemsSource = ListOfQuizzes;
            settingsPage = new Settings();
        }

        async void OnTaskTapped(object sender, ItemTappedEventArgs e)
        {
            ListView view = (ListView)sender;
            var think = (ThinkAndDo)view.SelectedItem;
            ThinkAndDoPopup pop = new ThinkAndDoPopup(think);
            await PopupNavigation.Instance.PushAsync(pop);
        }

        async void OnQuizTapped(object sender, ItemTappedEventArgs e)
        {
            //TO-DO in sprint 4
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