using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Security.Claims;
using Microsoft.AspNet.Identity;
using PlaceSharer.BLL.DTO;
using PlaceSharer.BLL.Infrastructure;
using PlaceSharer.BLL.Interfaces;
using PlaceSharer.DAL.Entities;
using PlaceSharer.DAL.Interfaces;
using AutoMapper;

namespace PlaceSharer.BLL.Services
{
    public class SubscriptionService : ISubscriptionService
    {
        IUnitOfWork Database;

        public SubscriptionService(IUnitOfWork db)
        {
            Database = db;
        }

        public async Task<OperationDetails> CreateAsync(SubscriptionDTO subscriptionDTO)
        {
            ApplicationUser currentUser = await Database.UserManager.FindByIdAsync(subscriptionDTO.SubscriberId);
            if(currentUser != null)
            {
                Subscription subscription = new Subscription
                {
                    SubscriberId = subscriptionDTO.SubscriberId,
                    SubscriptionUserId = subscriptionDTO.SubscriptionUserId
                };

                Database.SubscriptionManager.Create(subscription);
                await Database.SaveAsync();
                return new OperationDetails(true, "Subscription added", "");
            }
            return new OperationDetails(false, "User with this Id already exists", "Id");
        }
    }
}