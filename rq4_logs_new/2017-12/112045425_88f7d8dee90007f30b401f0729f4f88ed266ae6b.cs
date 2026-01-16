using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataStructures;

namespace UnitTestProject1
{
	[TestClass]
	public class TrieTests
	{
		[TestMethod]
		public void TestTrieConstructor()
		{
			Trie trie = new Trie();
			Assert.AreEqual(trie.Head.Depth, -1);
		}

		[TestMethod]
		public void TestTrieInsertOnce()
		{
			Trie trie = new Trie();
			trie.Insert("hi");
			Assert.AreEqual(trie.Head.Children['h'].Children['i'].IsEnd, true);
		}

		[TestMethod]
		public void TestTrieContainsPositive()
		{
			Trie trie = new Trie();
			trie.Insert("hi");
			Assert.AreEqual(trie.Contains("hi"), true);
		}

		[TestMethod]
		public void TestTrieContainsNegative()
		{
			Trie trie = new Trie();
			Assert.AreEqual(trie.Contains("hi"), false);
		}

		[TestMethod]
		public void TestTrieSize()
		{
			Trie trie = new Trie();
			trie.Insert("hi");
			Assert.AreEqual(trie.Contains("hello"), false);
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void TestTrieRemoveEmpty()
		{
			Trie trie = new Trie();
			trie.Remove("hi");
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentException))]
		public void TestTrieRemoveEmptyString()
		{
			Trie trie = new Trie();
			trie.Insert("hi");
			trie.Remove("");
		}

		[TestMethod]
		public void TestTrieRemovePositive()
		{
			Trie trie = new Trie();
			trie.Insert("hi");
			trie.Remove("hi");
			Assert.AreEqual(trie.Contains("hi"), false);
		}

		[TestMethod]
		public void TestTrieRemovePositiveBranches()
		{
			Trie trie = new Trie();
			trie.Insert("hi");
			trie.Insert("ha");
			trie.Remove("hi");
			Assert.AreEqual(trie.Contains("ha"), true);
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void TestTrieRemoveNegative()
		{
			Trie trie = new Trie();
			trie.Insert("hi");
			trie.Remove("ha");
		}
	}
}