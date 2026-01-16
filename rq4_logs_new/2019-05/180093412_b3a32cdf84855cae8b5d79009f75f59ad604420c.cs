using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PDGT.ViewModels;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace PDGT.Pages
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class SessionActivePage : ContentPage
	{
        SessionViewModel _vm;
        int sessionCount;
        bool sessionsDone;


        public SessionActivePage ()
		{
			InitializeComponent ();

            BindingContext = _vm = new SessionViewModel();

            CarouselViews.ItemsSource = _vm._mockService.Exercises;
                        
            SessionTitle.Text = "Session N";

            sessionCount = _vm._mockService.Exercises.Where(Name => Name != null).Count();
            exerciseCount.Text = sessionCount.ToString();

           
            bool sessionsDone = _vm._mockService.SessionsFinished;
            if (sessionsDone == false)
            {
                exerciseDone.Text = "0";
            }
            
		}
	}
}