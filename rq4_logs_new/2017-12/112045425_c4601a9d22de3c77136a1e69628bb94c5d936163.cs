using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Algorithms
{
	public class Sorter<T>
	{
		public Comparison<T> Comparison { get; set; }

		public Sorter(Comparison<T> comparison)
		{
			Comparison = comparison;
		}

		public IEnumerable<T> MergeSort(IEnumerable<T> values)
		{
			if (values.Count() < 2)
			{
				return values;
			}
			List<T> v1 = MergeSort(values.Take(values.Count() / 2)).ToList();
			List<T> v2 = MergeSort(values.Skip(values.Count() / 2)).ToList();
			List<T> ret = new List<T>();
			while (v1.Count > 0 && v2.Count > 0)
			{
				if (Comparison(v1.First(), v2.First()) >= 0)
				{
					ret.Add(v1.First());
					v1.RemoveAt(0);
				}
				else
				{
					ret.Add(v2.First());
					v2.RemoveAt(0);
				}
			}
			if (v1.Count > 0)
			{
				ret.AddRange(v1);
			}
			if (v2.Count > 0)
			{
				ret.AddRange(v2);
			}
			return ret;
		}
	}
}