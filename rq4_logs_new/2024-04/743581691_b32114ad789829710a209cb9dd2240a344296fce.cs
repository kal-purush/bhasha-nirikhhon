using Authenticate_Service.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace AuthenticateService.API.Feature.AuthenticateFearture.Command.Users.Querry
{
    public class QuantityOfStudentQuerry : IRequest<IActionResult>
    {

        public class QuantityOfStudentQuerryHandler : IRequestHandler<QuantityOfStudentQuerry, IActionResult>
        {
            private readonly AuthenticationContext _dbContext;

            public QuantityOfStudentQuerryHandler(AuthenticationContext dbContext)
            {
                _dbContext = dbContext;
            }
            public async Task<IActionResult> Handle(QuantityOfStudentQuerry request, CancellationToken cancellationToken)
            {
                var querry = await _dbContext.Users.Where(u => u.Role.Id == 1).ToListAsync();
                if (querry == null)
                {
                    return new OkObjectResult(0);
                }
                var count = querry.Count();

                return new OkObjectResult(count);
            }
        }
    }
}