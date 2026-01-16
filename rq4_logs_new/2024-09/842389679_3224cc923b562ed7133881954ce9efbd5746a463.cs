using Application.Common.Models;
using Application.Interfaces;
using MediatR;
using Domain.Entities;
using Domain.Enum;
using Microsoft.EntityFrameworkCore;

namespace Application.Modules.Commands
{
    public class UpdateModuleStatusCommand : IRequest<Result>
    {
        public int ModuleId { get; set; }
        public string ModuleStatus { get; set; }  
    }

    public class UpdateModuleStatusCommandHandler : IRequestHandler<UpdateModuleStatusCommand, Result>
    {
        private readonly IApplicationDbContext _context;

        public UpdateModuleStatusCommandHandler(IApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<Result> Handle(UpdateModuleStatusCommand request, CancellationToken cancellationToken)
        {
            // Retrieve the module by ID
            Module? module = await _context.Modules
                .FirstOrDefaultAsync(m => m.Id == request.ModuleId, cancellationToken);

            if (module == null)
            {
                return Result.Failure("Module not found.");
            }

            // Convert string to ModuleStatus enum
            if (!Enum.TryParse(request.ModuleStatus, true, out ModuleStatus status))
            {
                return Result.Failure("Invalid module status.");
            }

            // Update the module status
            module.ModuleStatus = status;
            module.ModuleStatusDes = request.ModuleStatus; 

            _context.Modules.Update(module);
            await _context.SaveChangesAsync(cancellationToken);

            return Result.Success($"Module status updated to {request.ModuleStatus}.");
        }
    }
}