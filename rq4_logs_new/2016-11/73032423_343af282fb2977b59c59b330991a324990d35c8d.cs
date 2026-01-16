using System;
using System.Collections.Generic;
using System.Web;

namespace YouZhiWenJiao.Web.Manage.Entity
{
	public class Information
	{
		private string _id = "";
		public string ID
		{
			get { return this._id; }
			set { this._id = value; }
		}

		private string _title = "";
		public string Title
		{
			get { return this._title; }
			set { this._title = value; }
		}

		private string _content = "";
		public string Content
		{
			get { return this._content; }
			set { this._content = value; }
		}

		private string _number = "";
		public string Number
		{
			get { return this._number; }
			set { this._number = value; }
		}

		private string _dateTime = "";
		public string DateTime
		{
			get { return this._dateTime; }
			set { this._dateTime = value; }
		}

		private string _type = "";
		public string Type
		{
			get { return this._type; }
			set { this._type = value; }
		}

		private string _showPic = "";
		public string ShowPic
		{
			get { return this._showPic; }
			set { this._showPic = value; }
		}

		private string _showInHomePage = "";
		public string ShowInHomePage
		{
			get { return this._showInHomePage; }
			set { this._showInHomePage = value; }
		}
	}
}