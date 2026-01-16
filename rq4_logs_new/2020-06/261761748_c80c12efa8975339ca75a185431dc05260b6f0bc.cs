using Microsoft.AspNetCore.Mvc;
using OrdersAPI.Models;
using OrdersAPI.Repository;
using System;
using System.Collections.Generic;
using Xunit;

namespace OrdersAPI.Tests
{
    public class RepositoryTests
    {
        [Fact]
        public void GetOrderById_Returns_Order()
        {
            var ordersRepository = new OrdersRepository();
            var added = ordersRepository.Add(new Order
            {
                UserId = Guid.NewGuid(),
                ProductId = Guid.NewGuid(),
                Amount = 5
            });
            var order = ordersRepository.GetById(added.Id);

            Assert.IsType<Order>(order);
            Assert.Equal(added.Id, order.Id);
        }

        [Fact]
        public void GetByUserId_Returns_Orders()
        {
            var ordersRepository = new OrdersRepository();
            var added = ordersRepository.Add(new Order
            {
                UserId = Guid.NewGuid(),
                ProductId = Guid.NewGuid(),
                Amount = 5
            });
            var orders = ordersRepository.GetByUserId(added.UserId);
            Assert.Contains(orders, o => o.UserId == added.UserId);
        }

        [Fact]
        public void GetByProductId_Returns_Orders()
        {
            var ordersRepository = new OrdersRepository();
            var added = ordersRepository.Add(new Order
            {
                UserId = Guid.NewGuid(),
                ProductId = Guid.NewGuid(),
                Amount = 5
            });
            var orders = ordersRepository.GetByProductId(added.ProductId);
            Assert.Contains(orders, o => o.ProductId == added.ProductId);
        }

        [Fact]
        public void GetAll_Returns_Orders()
        {
            var ordersRepository = new OrdersRepository();

            var orders = ordersRepository.GetAll();
            var orderIds = new List<Guid>();

            bool uniqueIds = true;
            foreach (var order in orders)
            {
                if (orderIds.Contains(order.Id))
                {
                    uniqueIds = false;
                    break;
                }
                orderIds.Add(order.Id);
            }
            Assert.True(uniqueIds);
        }

        [Fact]
        public void AddOrder_Adds_Order()
        {
            var ordersRepository = new OrdersRepository();
            var order = ordersRepository.Add(new Order
            {
                UserId = Guid.NewGuid(),
                ProductId = Guid.NewGuid(),
                Amount = 5
            });

            //assumes GetById works
            var dbOrder = ordersRepository.GetById(order.Id);
            Assert.Equal(order, dbOrder);
        }

        [Fact]
        public void RemoveById_Removes_Order()
        {
            var ordersRepository = new OrdersRepository();
            // add an order to remove
            var order = ordersRepository.Add(new Order
            {
                UserId = Guid.NewGuid(),
                ProductId = Guid.NewGuid(),
                Amount = 5
            });

            ordersRepository.RemoveById(order.Id);
            // assumes GetAll works
            var orders = ordersRepository.GetAll();
            Assert.DoesNotContain(orders, o => o.Id == order.Id);
        }
    }
}