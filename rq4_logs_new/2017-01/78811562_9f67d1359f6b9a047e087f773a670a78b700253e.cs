using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace NewellClark.Wpf.UserControls
{
	/// <summary>
	/// A property that can only be set in the designer, and once during run-time, after which
	/// it will throw an InvalidOperationException.
	/// </summary>
	/// <typeparam name="T">The type of the enclosed property.</typeparam>
	internal class DesignerOnlyProperty<T>
	{
		/// <summary>
		/// Creates a new <c>DesignerOnlyProperty</c> that is owned by the specified <c>DependencyObject</c>,
		/// with the specified getter and setter delegates.
		/// </summary>
		/// <param name="owner">The <c>DependencyObject</c> that owns the property.</param>
		/// <param name="getter">The delegate that will be called to get the property.</param>
		/// <param name="setter">The delegate that will be used to set the property.</param>
		public DesignerOnlyProperty(DependencyObject owner, Func<T> getter, Action<T> setter)
		{
			_owner = owner;
			_getter = getter;
			_setter = setter;
		}

		/// <summary>
		/// Sets the value of the property. This method can be called as many times as you like inside the 
		/// designer. It can only be called once at run-time. Subsequent calls will throw an
		/// <c>InvalidOperationException</c>.
		/// </summary>
		/// <param name="value"></param>
		/// <param name="callerMemberName"></param>
		public void Set(T value, [CallerMemberName]string callerMemberName = "")
		{
			if (!DesignerProperties.GetIsInDesignMode(_owner))
			{
				if (_hasBeenSetOutsideOfDesigner)
				{
					ThrowException(callerMemberName);
				}
				_hasBeenSetOutsideOfDesigner = true;
			}
			_setter(value);
		}

		public T Get()
		{
			return _getter();
		}

		private void ThrowException(string propertyName)
		{
			throw new InvalidOperationException(GetExceptionMessage(propertyName));
		}

		private string GetExceptionMessage(string propertyName)
		{
			return $"The property '{propertyName}` can only be set inside the designer (and it can be set " +
				$"once at run-time, during initialization).";
		}

		private Func<T> _getter;
		private Action<T> _setter;
		private bool _hasBeenSetOutsideOfDesigner = false;
		private DependencyObject _owner;
		
	}
}