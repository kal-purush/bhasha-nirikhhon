using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataStructures;

namespace UnitTestProject1
{
	[TestClass]
	public class LinkedListTests
	{
		[TestMethod]
		public void TestLinkedListPush()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Head.Next.Next.Data, "first");
		}

		[TestMethod]
		public void TestLinkedListPopReturnsValue()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Pop(), "third");
		}

		[TestMethod]
		public void TestLinkedListPopResetsHead()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Pop();
			Assert.AreEqual(list.Head.Data, "second");
		}

		[TestMethod]
		public void TestLinkedListSizeEmpty()
		{
			LinkedList<string> list = new LinkedList<string>();
			Assert.AreEqual(list.Size(), 0);
		}

		[TestMethod]
		public void TestLinkedListSizeFull()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Size(), 3);
		}

		[TestMethod]
		public void TestLinkedListSearchPositive()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Search("second"), list.Head.Next);
		}

		[TestMethod]
		public void TestLinkedListSearchNegative()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			var thing = list.Search("fourth");
			Assert.AreEqual(list.Search("fourth"), null);
		}

		[TestMethod]
		public void TestLinkedListRemoveHeadPositive()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Remove(list.Head);
			Assert.AreEqual(list.Head.Data, "second");
		}

		[TestMethod]
		public void TestLinkedListRemovePositive()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Remove(list.Head.Next);
			Assert.AreEqual(list.Head.Next.Data, "first");
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void TestLinkedListRemoveHeadNegative()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Remove(list.Head);
		}

		[TestMethod]
		public void TestLinkedListRemoveNegative()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Remove(new LinkedListNode<string>(""));
			Assert.AreEqual(list.Head.Next.Next.Data, "first");
		}

		[TestMethod]
		public void TestLinkedListDisplayEmpty()
		{
			LinkedList<string> list = new LinkedList<string>();
			Assert.AreEqual(list.Display(), "()");
		}

		[TestMethod]
		public void TestLinkedListDisplayFull()
		{
			LinkedList<string> list = new LinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Push(null);
			Assert.AreEqual(list.Display(), "(<null>, third, second, first)");
		}
	}
}