using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CarService.AppCore.Interfaces;
using CarService.Domain.Models;
using MediatR;

namespace CarService.AppCore.Cqrs.Commands.Handlers
{
    public class CreateRepairOrderCommandHandler : IRequestHandler<CreateRepairOrderCommand, RepairOrder>
    {
        private readonly IRepairOrdersRepository _repairOrdersRepository;

        public CreateRepairOrderCommandHandler(IRepairOrdersRepository repairOrdersRepository)
        {
            _repairOrdersRepository = repairOrdersRepository;
        }

        public async Task<RepairOrder> Handle(CreateRepairOrderCommand request, CancellationToken cancellationToken)
        {
            var repairOrder = new RepairOrder
            {
                Id = request.Id,
                Price = request.Price,
                OrderDate = request.OrderDate,
                CarId = request.CarId
            };

            var response = await _repairOrdersRepository.Create(repairOrder);

            return response;
        }
    }
}