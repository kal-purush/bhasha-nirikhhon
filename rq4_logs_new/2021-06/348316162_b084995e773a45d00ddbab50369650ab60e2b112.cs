using EventPlanner.Models;
using EventPlanner.Services;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventPlanner.ViewModels
{
    class NavBarViewModel : ObservableObject
    {
        public bool IsOrganizer => UserService.Singleton().CurrentUser is Organizer;
        public bool IsUser
        {
            get
            {
                User user = UserService.Singleton().CurrentUser;
                return user is User && !(user is Admin) && !(user is Organizer);
            }
        }
        public bool IsAdmin => UserService.Singleton().CurrentUser is Admin;
        public bool IsLoggedIn => UserService.Singleton().CurrentUser != null;
        
        public bool IsNotAdmin
        {
            get
            {
                User user = UserService.Singleton().CurrentUser;
                return user is User && !(user is Admin);
            }
        }

        public NavBarViewModel()
        {
            UserService.Singleton().LoggedInUserChanged += NavBarViewModel_LoggedIn;
        }

        private void NavBarViewModel_LoggedIn(object sender, EventArgs e)
        {
            RaisePropertyChngedEvent("IsOrganizer");
            RaisePropertyChngedEvent("IsAdmin");
            RaisePropertyChngedEvent("IsUser");
            RaisePropertyChngedEvent("IsNotAdmin");
            RaisePropertyChngedEvent("IsLoggedIn");
        }
    }
}