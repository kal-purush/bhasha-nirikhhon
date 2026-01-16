using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace AvSBookStore.Tests
{
    public class OrderItemTests
    {
        [Fact]
        public void OrderItemWIthZeroCountThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
           {
               int count = 0;
               new OrderItem(1, count, 0m);
           });
        }

        [Fact]
        public void OrderItemWIthNegativeCountThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                int count = -1;
                new OrderItem(1, count, 0m);
            });
        }

        [Fact]
        public void OrderItemWIthPositiveCountSetsCount()
        {
            OrderItem orderItem = new OrderItem(1, 2, 3m);

            Assert.Equal(1, orderItem.BookId);
            Assert.Equal(2, orderItem.Count);
            Assert.Equal(3m, orderItem.Price);
        }
    }
}