using CarService.AppCore.Cqrs.Commands;
using CarService.AppCore.Interfaces;
using CarService.AppCore.Models.EventModels;
using CarService.AppCore.Models.Events;
using CarService.Domain.Models;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CarService.AppCore.Services
{
    public class CommandFactory : ICommandFactory
    {
        public IBaseRequest CreateCommand<T>(BaseEvent<T> @event, RepairOrderRedisEventDataModel eventData) where T : class
        {
            switch (@event)
            {
                case RepairOrderCreatedEvent:
                    return new CreateRepairOrderCommand
                    {
                        Id = eventData.Id,
                        Price = eventData.Price,
                        CarId = eventData.CarId,
                        OrderDate = eventData.OrderDate
                    };

                case RepairOrderUpdatedEvent:
                    return new UpdateRepairOrderCommand
                    {
                        Id = eventData.Id,
                        CarId = eventData.CarId,
                        OrderDate = eventData.OrderDate,
                        Price = eventData.Price
                    };

                case RepairOrderDeletedEvent:
                    return new DeleteRepairOrderCommand { Id = eventData.Id };

                default:
                    return null;
            }
        }
    }
}