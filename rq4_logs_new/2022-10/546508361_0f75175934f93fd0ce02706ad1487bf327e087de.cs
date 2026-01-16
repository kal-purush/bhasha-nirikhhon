using Application.FridgeProduct.Queries.GetFridgeProducts;
using Application.Interfaces;
using AutoMapper;
using Domain.DTOs;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Domain;

namespace Application.FridgeProduct.StoredProcedures.SPChangeZeroQuantity
{
    public class SPChangeZeroQuantityRequestHandler : IRequestHandler<SPChangeZeroQuantityRequest, Unit>
    {
        private readonly IFridgeDbContext _db;
        public SPChangeZeroQuantityRequestHandler(IFridgeDbContext db)
        {
            _db = db;
        }
        public async Task<Unit> Handle(SPChangeZeroQuantityRequest request, CancellationToken cancellationToken)
        {
            _db.ChangeZeroQuantity();

            return Unit.Value;
        }
    }
}