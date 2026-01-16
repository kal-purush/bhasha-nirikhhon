using System.Collections.Generic;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MediatR;
using MailKit.Net.Smtp;
using MimeKit;
using MailKit;
using NETCore.MailKit.Core;
using Microsoft.EntityFrameworkCore;
using RolleiShop.Data.Context;
using RolleiShop.Specifications;
using RolleiShop.Specifications.Interfaces;
using RolleiShop.Entities;
using RolleiShop.Services.Interfaces;


namespace RolleiShop.Notifications
{
    public class CustomerCheckoutNotification : INotification
    {
        public string Name { get; set; }

        public CustomerCheckoutNotification(string name)
        {
            Name = name;
        }
    }

    public class CustomerCheckoutNotificationHandler : INotificationHandler<CustomerCheckoutNotification>
    {
        public class Model
        {
            public int OrderNumber { get; set; }
            public DateTimeOffset OrderDate { get; set; }
            public List<OrderItem> OrderItems { get; set; } = new List<OrderItem>();
            public class OrderItem
            {
                public int ProductId { get; set; }
                public string ProductName { get; set; }
                public decimal UnitPrice { get; set; }
                public int Units { get; set; }
                public string ImageUrl { get; set; }
            }
        }

        private readonly ApplicationDbContext _context;
        private readonly IEmailService _emailService;

        public CustomerCheckoutNotificationHandler(ApplicationDbContext context,
                              IEmailService emailService)
        {
            _context = context;
            _emailService = emailService;
        }

        public async Task Handle(CustomerCheckoutNotification notification, CancellationToken cancellationToken)
        {
            var model = await FirstAsync(new CustomerOrdersWithItemsSpecification(notification.Name));
            /* model.OrderDate = model.OrderDate.ToString("dd/MM/yyyy"); */

            string BodyContent = $@"<p>{notification.Name},</p>
                <p>Thank you for your order!</p>
                <p> Your order confirmation number is {model.Id} on {model.OrderDate.ToString("dd/MM/yyyy")}.</p>";

            _emailService.Send("maikeulbgithub@gmail.com", "ASP.NET Core mvc send email example", BodyContent, true);
        }

        private async Task<List<Order>> ListAsync(ISpecification<Order> spec)
        {
            var queryableModelWithIncludes = spec.Includes
                .Aggregate(_context.Orders.AsQueryable(),
                    (current, include) => current.Include(include));
            var secondaryModel = spec.IncludeStrings
                .Aggregate(queryableModelWithIncludes,
                    (current, include) => current.Include(include));
            return await secondaryModel.Where(spec.Criteria)
                .AsNoTracking()
                .ToListAsync();
        }

        private async Task<Order> FirstAsync(ISpecification<Order> spec)
        {
            return await _context.Orders
                .AsNoTracking()
                .Where(spec.Criteria)
                .LastOrDefaultAsync();
        }
    }
}