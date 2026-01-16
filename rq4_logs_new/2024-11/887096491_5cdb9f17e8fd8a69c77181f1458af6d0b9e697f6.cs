//using MediatR;
//using Microsoft.EntityFrameworkCore;
//using WolfDen.Domain.Entity;
//using WolfDen.Infrastructure.Data;

//namespace WolfDen.Application.Requests.Commands.LeaveManagement.LeaveRequests.AddLeaveRequest
//{
//    public class AddLeaveRequestCommandHandler(WolfDenContext context) : IRequestHandler<AddLeaveRequestCommand, string>
//    {
//        private readonly WolfDenContext _context = context;

//        public async Task<string> Handle(AddLeaveRequestCommand request, CancellationToken cancellationToken)
//        {
//            LeaveBalance leaveBalance = await _context.LeaveBalances.FirstOrDefaultAsync(x => x.EmployeeId == request.EmpId && x.TypeId == request.TypeId, cancellationToken);

//            if (leaveBalance == null)
//            {
//                throw new InvalidOperationException($"Leave balance not found for Employee ID {request.EmpId} and Type ID {request.TypeId}.");
//            }

//            LeaveType leaveType = await _context.LeaveType.FirstOrDefaultAsync(x => x.Id == request.TypeId);
//            int days = 0;

//            if (leaveType.Sandwich == false)
//            {
//                request.FromDate - request.ToDate
//            }
//        }
//    }
//}