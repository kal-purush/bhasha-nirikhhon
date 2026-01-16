using Unity;
using Unity.ServiceLocation;
using CommonServiceLocator;

using NotifyMe.ServiceInterfaces;
using NotifyMe.Services;
using NotifyMe.ViewModels;

namespace NotifyMe
{
    public class DependencyInjector
    {
        private readonly UnityContainer _unityContainer;

        public DependencyInjector()
        {
            _unityContainer = new UnityContainer();

            _unityContainer.RegisterType<IUserService, UserService>();

            _unityContainer.RegisterType<LoginViewModel>();
            _unityContainer.RegisterType<SignupViewModel>();

            ServiceLocator.SetLocatorProvider(() => new UnityServiceLocator(_unityContainer));
        }

        
    }
}