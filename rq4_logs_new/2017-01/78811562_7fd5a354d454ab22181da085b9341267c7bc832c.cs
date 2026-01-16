using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace NewellClark.Wpf.UserControls
{
	internal static class Common
	{
		public static bool SetField<T>(
			this PropertyChangedEventHandler @this, 
			ref T field, T value,
			[CallerMemberName]string name = "")
		{
			if (EqualityComparer<T>.Default.Equals(field, value))
				return false;
			field = value;
			@this?.Invoke(@this.Target, new PropertyChangedEventArgs(name));
			return true;
		}

		
	}
}