using Application.Interfaces;
using AutoMapper;
using Domain.DTOs;
using MediatR;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Application.Fridges.Queries.GetFridges
{
    public class GetFridgesQueryHandler : IRequestHandler<GetFridgesQuery, IEnumerable<FridgeDto>>
    {
        private readonly IFridgeDbContext _db;
        private readonly IMapper _mapper;
        private readonly ILoggerManager _loggerManager;
        public GetFridgesQueryHandler(IFridgeDbContext db, IMapper mapper, ILoggerManager logger)
        {
            _db = db;
            _mapper = mapper;
            _loggerManager = logger;    
        }
        public async Task<IEnumerable<FridgeDto>> Handle(GetFridgesQuery request, CancellationToken cancellationToken)
        {
            var fridges = await _db.Fridges.ToListAsync();
            if (fridges == null)
            {
                _loggerManager.LogError("Fridges in the database is null.");
            }

            var fridgesDto = _mapper.Map<IEnumerable<FridgeDto>>(fridges);

            return fridgesDto;
        }
    }
}