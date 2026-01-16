using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataStructures;

namespace UnitTestProject1
{
	[TestClass]
	public class QueueTests
	{
		[TestMethod]
		public void TestQueueEnqueue()
		{
			Queue<string> queue = new Queue<string>();
			queue.Enqueue("first");
			queue.Enqueue("second");
			queue.Enqueue("third");
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void TestQueueDequeueEmpty()
		{
			Queue<string> queue = new Queue<string>();
			queue.Dequeue();
		}

		[TestMethod]
		public void TestQueueDequeueOnce()
		{
			Queue<string> queue = new Queue<string>();
			queue.Enqueue("first");
			queue.Enqueue("second");
			queue.Enqueue("third");
			Assert.AreEqual(queue.Dequeue(), "first");
		}

		[TestMethod]
		public void TestQueueDequeueTwice()
		{
			Queue<string> queue = new Queue<string>();
			queue.Enqueue("first");
			queue.Enqueue("second");
			queue.Enqueue("third");
			queue.Dequeue();
			Assert.AreEqual(queue.Dequeue(), "second");
		}

		[TestMethod]
		public void TestQueueSize()
		{
			Queue<string> queue = new Queue<string>();
			queue.Enqueue("first");
			queue.Enqueue("second");
			queue.Enqueue("third");
			Assert.AreEqual(queue.Size(), 3);
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void TestQueuePeekEmpty()
		{
			Queue<string> queue = new Queue<string>();
			Assert.IsNull(queue.Peek());
		}

		[TestMethod]
		public void TestQueuePeekOnce()
		{
			Queue<string> queue = new Queue<string>();
			queue.Enqueue("first");
			queue.Enqueue("second");
			queue.Enqueue("third");
			Assert.AreEqual(queue.Peek(), "first");
		}

		[TestMethod]
		public void TestQueuePeekTwice()
		{
			Queue<string> queue = new Queue<string>();
			queue.Enqueue("first");
			queue.Enqueue("second");
			queue.Enqueue("third");
			queue.Peek();
			Assert.AreEqual(queue.Peek(), "first");
		}
	}
}