using System;

namespace SimpleChurchApp.Shared
{
	public class ChurchEvent
	{
		private string _title;
		private DateTime _dateTime;

		public ChurchEvent (string title, DateTime dateTime)
		{
			_title = title;
			_dateTime = dateTime;
		}

		public string Title
		{
			get { return _title; }
		}

		public DateTime DateTime 
		{
			get { return _dateTime; }
		}
	}
}
