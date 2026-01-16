using MediatR;
using Microsoft.AspNetCore.Identity;
using WolfDen.Domain.Entity;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Commands.Employees.SyncEmployee
{
    public class SyncEmployeeCommandHandler(WolfDenContext context, UserManager<User> userManager) : IRequestHandler<SyncEmployeeCommand, bool>
    {
        private readonly WolfDenContext _context = context;
        private readonly UserManager<User> _userManager = userManager;
        public async Task<bool> Handle(SyncEmployeeCommand request, CancellationToken cancellationToken)
        {
            List<Employee> employees = _context.Employees.Where(x => x.UserId == null).ToList();
            if (employees.Any())
            {
                foreach (Employee employee in employees)
                {
                    User user = new User(employee.RFId);
                    await _userManager.CreateAsync(user);
                    await _userManager.AddToRoleAsync(user, "Employee");
                    employee.UpdateUserId(user.Id);
                    _context.Employees.Update(employee);

                }
                await _context.SaveChangesAsync();
                return true;
            }
            return false;
        }
    }
}