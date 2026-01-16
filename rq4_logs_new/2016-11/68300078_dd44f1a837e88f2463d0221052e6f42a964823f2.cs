using AutoMapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EnterpriseLayers.Service {
	public static class AutoMapperExtensions {
		public static T To<T>(this object source) {
			return Mapper.Map<T>(source);
		}

		public static List<T> To<T>(this List<object> source) {
			return Mapper.Map<List<T>>(source);
		}
	}
}