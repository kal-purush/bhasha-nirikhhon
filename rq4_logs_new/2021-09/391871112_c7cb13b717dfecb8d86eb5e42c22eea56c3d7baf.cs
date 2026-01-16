using ConasiCRM.Portable.ViewModels;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace ConasiCRM.Portable.Models
{
    public class ActionButton : BaseViewModel
    {
        private string _text;
        public string Text
        {
            get => _text;
            set
            {
                _text = value;
                OnPropertyChanged(nameof(Text));
            }
        }
        public Action<List<OptionSet>> Action { get; set; }

        public ActionButton()
        {
        }

        public ActionButton(string text, Action<List<OptionSet>> action)
        {
            Text = text;
            Action = action;
        }
    }
}