using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataStructures;

namespace UnitTestProject1
{
	[TestClass]
	public class HeapTests
	{
		[TestMethod]
		public void TestHeapConstructorEmpty()
		{
			Heap<string> heap = new Heap<string>((s, t) => s.Length > t.Length ? 1 : t.Length > s.Length ? -1 : 0);
			Assert.AreEqual(heap.Values.Count, 0);
		}

		[TestMethod]
		public void TestHeapConstructorFull()
		{
			Heap<string> heap = new Heap<string>((s, t) => s.Length > t.Length ? 1 : t.Length > s.Length ? -1 : 0,
				new string[] { "1", "22", "333", "4444", "55555" });
			Assert.AreEqual(heap.Values.Count, 5);
		}

		[TestMethod]
		public void TestHeapPush()
		{
			Heap<string> heap = new Heap<string>((s, t) => s.Length > t.Length ? 1 : t.Length > s.Length ? -1 : 0);
			heap.Push("4444");
			heap.Push("1");
			heap.Push("55555");
			heap.Push("333");
			heap.Push("22");
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void TestHeapPopEmpty()
		{
			Heap<string> heap = new Heap<string>((s, t) => s.Length > t.Length ? 1 : t.Length > s.Length ? -1 : 0);
			heap.Pop();
		}

		[TestMethod]
		public void TestHeapPopOnce()
		{
			Heap<string> heap = new Heap<string>((s, t) => s.Length > t.Length ? 1 : t.Length > s.Length ? -1 : 0);
			heap.Push("4444");
			heap.Push("1");
			heap.Push("55555");
			heap.Push("333");
			heap.Push("22");
			Assert.AreEqual(heap.Pop(), "55555");
		}

		[TestMethod]
		public void TestHeapPopTwice()
		{
			Heap<string> heap = new Heap<string>((s, t) => s.Length > t.Length ? 1 : t.Length > s.Length ? -1 : 0);
			heap.Push("4444");
			heap.Push("1");
			heap.Push("55555");
			heap.Push("333");
			heap.Push("22");
			heap.Pop();
			Assert.AreEqual(heap.Pop(), "4444");
		}
	}
}