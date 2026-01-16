using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataStructures
{
	class TrieNode
	{
		public char? Value { get; set; }
		public int Depth { get; set; }
		public TrieNode Parent { get; set; }
		public List<TrieNode> Children { get; set; }
		public bool IsEnd { get; set; }

		public TrieNode(char? val, int depth, TrieNode parent)
		{
			Value = val;
			Depth = depth;
			Parent = parent;
			Children = new List<TrieNode>();
			IsEnd = false;
		}
	}

	class Trie : IEnumerable<string>
	{
		public TrieNode Head { get; set; }

		public Trie()
		{
			Head = new TrieNode(null, -1, null);
		}

		/// <summary>
		/// Adds a branch to the trie corresponding to the string.
		/// </summary>
		/// <param name="val"></param>
		public void Insert(string val)
		{
			TrieNode parent = Head;
			for (int i = 0; i < val.Length; i++)
			{
				if (!parent.Children.Any(n => n.Value == val[i]))
				{
					TrieNode newParent = new TrieNode(val[i], i, parent);
					parent.Children.Add(newParent);
					parent = newParent;
				}
				else
				{
					parent = parent.Children.First(n => n.Value == val[i]);
				}
			}
			parent.IsEnd = true;
		}

		/// <summary>
		/// Returns whether or not a string is contained in the trie.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public bool Contains(string val)
		{
			TrieNode parent = Head;
			for (int i = 0; i < val.Length; i++)
			{
				if (!parent.Children.Any(n => n.Value == val[i]))
				{
					return false;
				}
				else
				{
					parent = parent.Children.First(n => n.Value == val[i]);
				}
			}
			return parent.IsEnd;
		}

		/// <summary>
		/// Returns the number of branches in the trie.
		/// </summary>
		/// <returns></returns>
		public int Size()
		{
			int count = 0;
			foreach (string val in this)
			{
				count++;
			}
			return count;
		}

		/// <summary>
		/// Removes a string from the trie.
		/// </summary>
		/// <param name="val"></param>
		public void Remove(string val)
		{
			TrieNode last = Head;
			char lastVal = ' ';
			bool cut = false;
			TrieNode parent = Head;
			for (int i = 0; i < val.Length; i++)
			{
				if (!parent.Children.Any(n => n.Value == val[i]) || (i == val.Length - 1 && !parent.Children.First(n => n.Value == val[i]).IsEnd))
				{
					throw new Exception(String.Format("value {0} doesn't exist in trie.", val));
				}
				if (parent.IsEnd || parent.Children.Count > 1)
				{
					last = parent;
					lastVal = val[i];
				}
				parent = parent.Children.First(n => n.Value == val[i]);
				if (i == val.Length - 1)
				{
					parent.IsEnd = false;
					if (parent.Children.Count == 0)
					{
						cut = true;
					}
				}
			}
			if (cut)
			{
				last.Children.Remove(last.Children.First(n => n.Value == lastVal));
			}
		}

		public IEnumerator<string> GetEnumerator()
		{
			StringBuilder bldr = new StringBuilder();
			Stack<TrieNode> stack = new Stack<TrieNode>();
			stack.Push(Head);
			while (stack.List.Size() > 0)
			{
				TrieNode cur = stack.Pop();
				bldr.Append(cur.Value);
				if (cur.IsEnd)
				{
					yield return bldr.ToString();
					bldr.Clear();
				}
				foreach (TrieNode node in cur.Children)
				{
					stack.Push(node);
				}
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}