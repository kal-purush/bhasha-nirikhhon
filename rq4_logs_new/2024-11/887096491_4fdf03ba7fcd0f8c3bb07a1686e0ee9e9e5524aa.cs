using MediatR;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WolfDen.Application.DTOs.Employees;
using WolfDen.Application.Requests.Queries.Employees.GetAllDepartment;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Queries.Designation
{
    public class GetAllDesignationQueryHandler: IRequestHandler<GetAllDesignationQuery, List<DesignationDTO>>
    {
        private readonly WolfDenContext _context;

        public GetAllDesignationQueryHandler(WolfDenContext context)
        {
            _context = context;
        }

        public async Task<List<DesignationDTO>> Handle(GetAllDesignationQuery query, CancellationToken cancellationToken)
        {
            var departments = await _context.Designations
              .Select(designation => new DesignationDTO
              {
                  Id = designation.Id,
                  Name = designation.Name
              })
              .ToListAsync(cancellationToken);

            return departments;

        }

    }
}