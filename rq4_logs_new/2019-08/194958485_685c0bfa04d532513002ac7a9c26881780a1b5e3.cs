using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Taskio.HelperViewModels
{
    public class ExplandableListElementViewModel: INotifyPropertyChanged
    {
        public string ButtonDisplay { get; set;}
        public string LabelDisplay { get; set;}
        /// <summary>
        /// Viewmodel that binds list element and expandable list
        /// </summary>
        /// <param name="name">LabelName</param>
        /// <param name="buttonName"> button name</param>
        public ExplandableListElementViewModel(string name,string buttonName)
        {
            ButtonDisplay = buttonName;
            LabelDisplay = name;
        }

        public event PropertyChangedEventHandler PropertyChanged;
    }
}