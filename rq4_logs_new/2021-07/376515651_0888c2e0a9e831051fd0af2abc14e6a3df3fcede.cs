using CarService.AppCore.Models.EventModels;
using CarService.AppCore.Models.Events;
using CarService.RepairOrders.Messenger.Cqrs.Commands;
using CarService.RepairOrders.Messenger.Interfaces;
using MediatR;
using Newtonsoft.Json.Linq;

namespace CarService.RepairOrders.Messenger.Services
{
    public class CommandFactory : ICommandFactory
    {
        public IBaseRequest CreateCommand<T>(BaseEvent<T> @event) where T : JObject
        {
            switch (@event.Type)
            {
                case "RepairOrderCreatedEvent":
                {
                    var eventDataModel = @event.Data.ToObject<RepairOrderCreatedDataModel>();

                    return new CreateRepairOrderCommand
                    {
                        Id = eventDataModel.Id,
                        Price = eventDataModel.Price,
                        CarId = eventDataModel.CarId,
                        OrderDate = eventDataModel.OrderDate,
                    };
                }
                   
                case "RepairOrderUpdatedEvent":
                {
                    var eventDataModel = @event.Data.ToObject<RepairOrderUpdatedDataModel>();

                    return new UpdateRepairOrderCommand
                    {
                        Id = eventDataModel.Id,
                        CarId = eventDataModel.CarId,
                        OrderDate = eventDataModel.OrderDate,
                        Price = eventDataModel.Price
                    };
                }
                    
                case "RepairOrderDeletedEvent":
                {
                    var eventDataModel = @event.Data.ToObject<RepairOrderDeletedDataModel>();

                    return new DeleteRepairOrderCommand { Id = eventDataModel.Id };
                }
                
                default:
                    return null;
            }
        }
    }
}