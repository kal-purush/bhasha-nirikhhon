using System;
using Xunit;
using LinkedList.Classes;
using LLKthFromTheEnd;

namespace KthFromTheEndTests
{
    public class KthFromTheEndMethodTests
    {
        [Fact]
        public void FindKInList()
        {
            LList lL = new LList();
            lL.Insert(1);
            lL.Insert(2);
            lL.Insert(3);
            lL.Insert(4);

            int k = 2;
            Assert.Equal(3, Program.KthFromTheEnd(lL, k));
        }
        [Fact]
        public void ThrowExceptionInEmptyList()
        {
            LList lL = new LList();

            int k = 2;
            Assert.Throws<Exception>(() => Program.KthFromTheEnd(lL, k));
        }
        [Fact]
        public void ThrowExceptionWhenKAndLengthAreNotCompatible()
        {
            LList lL = new LList();

            lL.Insert(1);
            lL.Insert(2);
            lL.Insert(3);
            lL.Insert(4);

            int k = 10;
            Assert.Throws<Exception>(() => Program.KthFromTheEnd(lL, k));
        }
    }
}