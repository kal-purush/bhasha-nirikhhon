using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using TheShop.Model;
using TheShop.MVVM.ViewModel;

namespace TheShop.MVVM.Wrapper
{
	public class ProductWrapper : ViewModelBase, INotifyDataErrorInfo
	{
		public ProductWrapper(Product model)
		{
			Model = model;
		}

		public Product Model { get; set; }

		public int Id { get { return Model.Id; } }

		public string Name
		{
			get { return Model.Name; }
			set
			{
				Model.Name = value;
				OnPropertyChanged();
			}
		}

		public string Description
		{
			get { return Model.Description; }
			set
			{
				Model.Description = value;
				OnPropertyChanged();
			}
		}

		private Dictionary<string, List<string>> _errorsByPropertyName =
			new Dictionary<string, List<string>>();

		public bool HasErrors => _errorsByPropertyName.Any();

		public event EventHandler<DataErrorsChangedEventArgs> ErrorsChanged;

		public IEnumerable GetErrors(string propertyName)
		{
			return _errorsByPropertyName.ContainsKey(propertyName) ? _errorsByPropertyName[propertyName] : null;
		}

		private void OnErrorsChanged(string propertyName)
		{
			ErrorsChanged?.Invoke(this, new DataErrorsChangedEventArgs(propertyName));
		}

		private void AddError(string propertyName, string error)
		{
			if (!_errorsByPropertyName.ContainsKey(propertyName))
			{
				_errorsByPropertyName[propertyName] = new List<string>();
			}
			if (!_errorsByPropertyName[propertyName].Contains(error))
			{
				_errorsByPropertyName[propertyName].Add(error);
				OnErrorsChanged(propertyName);
			}
		}

		private void ClearErrors(string propertyName)
		{
			if (_errorsByPropertyName.ContainsKey(propertyName))
			{
				_errorsByPropertyName.Remove(propertyName);
				OnErrorsChanged(propertyName);
			}
		}
	}
}