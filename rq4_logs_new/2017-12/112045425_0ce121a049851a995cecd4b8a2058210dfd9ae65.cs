using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataStructures;

namespace UnitTestProject1
{
	[TestClass]
	public class DoublyLinkedListTests
	{
		[TestMethod]
		public void TestDoublyLinkedListPush()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Head.Next.Next.Data, "first");
		}

		[TestMethod]
		public void TestDoublyLinkedListAppend()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Append("first");
			list.Append("second");
			list.Append("third");
			Assert.AreEqual(list.Tail.Previous.Previous.Data, "first");
		}

		[TestMethod]
		public void TestDoublyLinkedListPopReturnsValue()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Pop(), "third");
		}

		[TestMethod]
		public void TestDoublyLinkedListPopResetsHead()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Pop();
			Assert.AreEqual(list.Head.Data, "second");
		}

		[TestMethod]
		public void TestDoublyLinkedListShiftReturnsValue()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Append("first");
			list.Append("second");
			list.Append("third");
			Assert.AreEqual(list.Shift(), "third");
		}

		[TestMethod]
		public void TestDoublyLinkedListShiftResetsTail()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Append("first");
			list.Append("second");
			list.Append("third");
			list.Shift();
			Assert.AreEqual(list.Tail.Data, "second");
		}

		[TestMethod]
		public void TestDoublyLinkedListSearchPositive()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Search("second"), list.Head.Next);
		}

		[TestMethod]
		public void TestDoublyLinkedListSearchNegative()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			Assert.AreEqual(list.Search("fourth"), null);
		}

		[TestMethod]
		public void TestDoublyLinkedListRemoveHeadPositive()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Remove(list.Head);
			Assert.AreEqual(list.Head.Data, "second");
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void TestDoublyLinkedListRemoveHeadNegative()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Remove(list.Head);
		}

		[TestMethod]
		public void TestDoublyLinkedListRemoveTailPositive()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Append("first");
			list.Append("second");
			list.Append("third");
			list.Remove(list.Tail);
			Assert.AreEqual(list.Tail.Data, "second");
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void TestDoublyLinkedListRemoveTailNegative()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Remove(list.Tail);
		}

		[TestMethod]
		public void TestDoublyLinkedListRemovePositive()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Remove(list.Head.Next);
			Assert.AreEqual(list.Head.Next.Data, "first");
		}

		[TestMethod]
		public void TestDoublyLinkedListRemoveNegative()
		{
			DoublyLinkedList<string> list = new DoublyLinkedList<string>();
			list.Push("first");
			list.Push("second");
			list.Push("third");
			list.Remove(new DoublyLinkedListNode<string>(""));
			Assert.AreEqual(list.Head.Next.Next.Data, "first");
		}
	}
}