using MediatR;
using Microsoft.AspNetCore.Identity;
using WolfDen.Domain.Entity;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Commands.Employees.SuperAdminUpdateEmployee
{
    public class SuperAdminUpdateEmployeeCommandHandler(WolfDenContext context, UserManager<User> userManager) : IRequestHandler<SuperAdminUpdateEmployeeCommand, bool>
    {
        private readonly WolfDenContext _context = context;
        private readonly UserManager<User> _userManager = userManager;

        public async Task<bool> Handle(SuperAdminUpdateEmployeeCommand request, CancellationToken cancellationToken)
        {
            Employee employee = await _context.Employees.FindAsync(request.Id, cancellationToken);
            if (employee == null)
            {
                return false;
            }
            using var transaction = await _context.Database.BeginTransactionAsync(cancellationToken);
            try
            {
                var user = await _userManager.FindByIdAsync(employee.UserId);
                var currentRoles = await _userManager.GetRolesAsync(user);
                if (currentRoles != null)
                {
                    await _userManager.RemoveFromRolesAsync(user, currentRoles);
                }
                var result = await _userManager.AddToRoleAsync(user, request.Role);
                await transaction.CommitAsync(cancellationToken);
                return true;
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw new Exception("Transaction Failed ");
            }
        }
    }
}