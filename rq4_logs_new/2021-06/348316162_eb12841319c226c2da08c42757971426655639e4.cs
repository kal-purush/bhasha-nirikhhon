using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventPlanner.Models
{
    class Table : ObservableObject
    {
        private string _Name;
        private Invites _Invites;

        public string Name
        {
            get => _Name;
            set { _Name = value; RaisePropertyChngedEvent("Name"); }
        }

        public Invites Invites
        {
            get => _Invites;
            set { _Invites = value; RaisePropertyChngedEvent("Invites"); }
        }
    }
}