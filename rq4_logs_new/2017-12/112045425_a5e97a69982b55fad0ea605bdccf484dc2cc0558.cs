using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataStructures;

namespace UnitTestProject1
{
	[TestClass]
	public class StackTests
	{
		[TestMethod]
		public void TestStackPush()
		{
			Stack<string> stack = new Stack<string>();
			stack.Push("first");
			stack.Push("second");
			stack.Push("third");
			Assert.AreEqual(stack.List.Head.Next.Next.Data, "first");
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void TestStackPopEmpty()
		{
			Stack<string> stack = new Stack<string>();
			stack.Pop();
		}

		[TestMethod]
		public void TestStackPopOnce()
		{
			Stack<string> stack = new Stack<string>();
			stack.Push("first");
			stack.Push("second");
			stack.Push("third");
			Assert.AreEqual(stack.Pop(), "third");
		}

		[TestMethod]
		public void TestStackPopTwice()
		{
			Stack<string> stack = new Stack<string>();
			stack.Push("first");
			stack.Push("second");
			stack.Push("third");
			stack.Pop();
			Assert.AreEqual(stack.Pop(), "second");
		}
	}
}