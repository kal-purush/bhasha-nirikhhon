using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;

namespace Homework_3.models
{
    class TodoModel: INotifyPropertyChanged
    {
        public DateTime CreationDate { get; set; } = DateTime.Now;

		private bool _isDone;
		private string _text;

		public bool IsDone
		{
			get { return _isDone; }
			set 
			{
				if (_isDone == value)
				{
					return;
				}
				_isDone = value;
				OnPropertyChange("IsDone");
			}
		}

		public string Text
		{
			get { return _text; }
			set 
			{
				if (_text == value)
				{
					return;
				}
				_text = value;
				OnPropertyChange("Text");
			}
		}

		public event PropertyChangedEventHandler PropertyChanged;

		protected virtual void OnPropertyChange(string propertyName = "")
		{
			PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
		}
	}
}