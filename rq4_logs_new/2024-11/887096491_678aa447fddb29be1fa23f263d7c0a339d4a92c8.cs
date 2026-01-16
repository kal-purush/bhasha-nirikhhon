using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MediatR;

namespace WolfDen.Application.Requests.Commands.LeaveManagement.LeaveRequests.AddLeaveRequest
{
    public class AddLeaveRequestCommandHandler : IRequestHandler<AddLeaveRequestCommand, string>
    {
        public Task<string> Handle(AddLeaveRequestCommand request, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}