using Microsoft.EntityFrameworkCore;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Helpers
{
    public class ManagerEmailFinder
    {
        private readonly WolfDenContext _context;

        public ManagerEmailFinder(WolfDenContext context)
        {
            _context = context;
        }
        public async Task<List<string>> FindManagerEmailsAsync(int? managerId, CancellationToken cancellationToken)
        {
            List<string> managerEmails = new List<string>();
            if (managerId == null) 
                return managerEmails;

            var manager = await _context.Employees
                .Where(m => m.Id == managerId)
                .Select(m => new { m.Email, m.ManagerId })
                .FirstOrDefaultAsync(cancellationToken);

            if (manager != null)
            {
                managerEmails.Add(manager.Email);
                List<string> higherManagerEmails = await FindManagerEmailsAsync(manager.ManagerId, cancellationToken);
                managerEmails.AddRange(higherManagerEmails);
            }
            return managerEmails;
        }
    }
}