using Application.Interfaces;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Web;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using Microsoft.AspNetCore.Mvc;

namespace Application.FridgeProduct.Commands.DeleteFridgeProduct
{
    public class DeleteFridgeProductCommandHandler : IRequestHandler<DeleteFridgeProductCommand>
    {
        private readonly IFridgeDbContext _db;
        private readonly ILoggerManager _logger;
        public DeleteFridgeProductCommandHandler(IFridgeDbContext db, ILoggerManager logger)
        {
            _db = db;
            _logger = logger;
        }
        public async Task<Unit> Handle(DeleteFridgeProductCommand request, CancellationToken cancellationToken)
        {
            _db.FridgeProducts.Remove(request.FridgeProduct);
            await _db.SaveChangesAsync(cancellationToken);
            return Unit.Value;
        }
    }
}