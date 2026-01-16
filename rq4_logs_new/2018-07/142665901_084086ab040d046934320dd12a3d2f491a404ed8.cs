using GalaSoft.MvvmLight.Ioc;
using GalaSoft.MvvmLight.Views;
using AirporClientUWP.ViewModels;
using CommonServiceLocator;

namespace AirporClientUWP
{
    public class ViewModelLocator
    {
        public ViewModelLocator()
        {
            ServiceLocator.SetLocatorProvider(() => SimpleIoc.Default);

            var navigationService = new NavigationService();
            //navigationService.Configure(nameof(AcademyViewModel), typeof(AcademyView));
            //navigationService.Configure(nameof(StudentViewModel), typeof(StudentView));

            //Register your services used here
            SimpleIoc.Default.Register<INavigationService>(() => navigationService);
            SimpleIoc.Default.Register<PilotViewModel>();

            //ServiceLocator.Current.GetInstance<StudentViewModel>();
        }
        
        public PilotViewModel PilotVMInstance
        {
            get
            {
                return ServiceLocator.Current.GetInstance<PilotViewModel>();
            }
        }
    }
}