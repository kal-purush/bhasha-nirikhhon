using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;

namespace AprioriAlgorithm
{
	public class ItemSet : SortedSet<int>
	{
		public ItemSet () : base()
		{
		}

		/* Because of below methods Equals() and GetHashCode(),
		 * two instances with same elements of the hash set 
		 * will be considered as same instances. */
		public override bool Equals(System.Object obj)
		{
			if (obj == null)
				return false;

			ItemSet iSet = obj as ItemSet;
			// If both has diffrent counts of elems, they are not equal
			if (this.Count != iSet.Count)
				return false;

			int[] arr1 = new int[this.Count];
			int[] arr2 = new int[iSet.Count];

			this.CopyTo(arr1);
			iSet.CopyTo(arr2);

			for(int i=0; i<this.Count; ++i)
			{
				if (arr1[i] != arr2[i])
					return false;
			}
			return true;
		}

		// meaningless method... 
		// just for distinguish two instances with different elements
		public override int GetHashCode()
		{
			int squareSum = 0;
			foreach(int item in this)
			{
				squareSum += item*item;
			}
			return squareSum;	// return sum of square of items
		}

		new public String ToString()
		{
			string toString = "";

			int length = this.Count;
			int i = 0;

			if (length > 0)
				toString += "{";
			
			foreach(int item in this)
			{
				if (++i == length)
					toString += item + "}";
				else
					toString += item + ",";
			}
			return toString;
		}

		/* Returns newly allocated instances with same items */
		public ItemSet SimpleClone()
		{
			ItemSet itemSet = new ItemSet();
			foreach(int item in this)
				itemSet.Add(item);
			return itemSet;
		}

		/* Returns the union set between item set I1, I2 */
		public static ItemSet UnionBetween (ItemSet I1, ItemSet I2)
		{
			ItemSet unionSet = I1.SimpleClone();
			unionSet.UnionWith(I2);
			return unionSet;
		}

		/* Caution: Algorithm to get all subsets. */
		/* Returns subsets of this item set. */
		public List<ItemSet> Subsets ()
		{
			List<ItemSet> subsetList = new List<ItemSet>();

			int[] thisArr = new int[this.Count];
			this.CopyTo(thisArr);

			int subsetCount = (int) Math.Pow(2, this.Count);
			for(int i=1; i<subsetCount-1; ++i)
			{
				ItemSet subSet = new ItemSet();
				for (int bitIndex=0; bitIndex<this.Count; ++bitIndex)
				{
					int bit = i & (int) Math.Pow(2, bitIndex);
					if (bit > 0)
						subSet.Add(thisArr[bitIndex]);
				}
				subsetList.Add(subSet);
			}
			return subsetList;
		}

		/* Returns (k-1)-subsets */
		public List<ItemSet> Subsets_k_1 ()
		{
			IList<int> list = new List<int>(this);
			List<ItemSet> setList = new List<ItemSet>();

			for (int i=0; i<list.Count; ++i)
			{
				ItemSet itemSet = new ItemSet();
				foreach (int item in list)
				{
					if (item != list[i])
						itemSet.Add(item);
				}
				setList.Add(itemSet);
			}
			return setList;
		}

		/* Returns this item set - item set 'sub' */
		public ItemSet Subtract (ItemSet sub)
		{
			ItemSet diffSet = this.SimpleClone();
			diffSet.ExceptWith(sub);
			return diffSet;
		}
	}
}