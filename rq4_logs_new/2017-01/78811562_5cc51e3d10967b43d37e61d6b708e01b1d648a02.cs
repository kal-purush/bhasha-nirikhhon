using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace UserControlsTests
{
	internal static class Common
	{
		public static void AssertPropertyChangedEventFires<T>(
			T target,
			Action<T> mutateProperty,
			bool assertNoUnspecifiedEventsFire,
			params string[] properties)
			where T : INotifyPropertyChanged
		{
			var unfiredEvents = new HashSet<string>(properties);
			target.PropertyChanged += (o, e) =>
			{
				bool specified = unfiredEvents.Remove(e.PropertyName);
				Assert.False(!specified && assertNoUnspecifiedEventsFire,
					$"Property {e.PropertyName} was not supposed to have a " +
					$"{nameof(INotifyPropertyChanged.PropertyChanged)} event raised for it, but it did.");
			};
			mutateProperty(target);

			Assert.IsEmpty(unfiredEvents,
				$"{nameof(INotifyPropertyChanged.PropertyChanged)} event failed to fire for " +
				$"the following properties: {ListStrings(unfiredEvents)}");
		}

		public static void AssertPropertyChangedEventFires<T>(
			T target,
			Action<T> mutateProperty,
			params string[] properties)
			where T : INotifyPropertyChanged
		{
			AssertPropertyChangedEventFires(target, mutateProperty, false, properties);
		}

		private static string ListStrings(IEnumerable<string> list)
		{
			var sb = new StringBuilder();
			foreach (string token in list)
			{
				sb.Append(token + " ");
			}

			return sb.ToString();
		}
	}
}