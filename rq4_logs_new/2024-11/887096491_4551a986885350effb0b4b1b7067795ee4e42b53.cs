//using Microsoft.EntityFrameworkCore;
//using Microsoft.Extensions.Configuration;
//using WolfDen.Application.Requests.Commands.Attendence.Email;
//using WolfDen.Domain.Entity;
//using WolfDen.Infrastructure.Data;

//namespace WolfDen.Application.Requests.Commands.Attendence.StatusUpdation
//{
//    public class StatusUpdateCommandHandler
//    {
//        private readonly WolfDenContext _context;
//        private readonly string SenderEmail;
//        private readonly string SenderName;
//        public StatusUpdateCommandHandler(WolfDenContext context, IConfiguration configuration)
//        {
//            _context = context;
//            SenderEmail = configuration["BrevoApi:SenderEmail"];
//            SenderName = configuration["BrevoApi:SenderName"];

//        }
//        public async Task<int> Handle(StatusUpdateCommand request, CancellationToken cancellationToken)
//        {
//            int statusId = 0;
//            var day = await _context.DailyAttendence.Where(x => x.EmployeeId == request.EmployeeId && x.Date == request.Date).FirstOrDefaultAsync(cancellationToken);
//            if (day == null)
//            {
//                var holiday = await _context.Holiday.Where(x => x.Date == request.Date).FirstOrDefaultAsync();
//                if (holiday == null)
//                {
//                    statusId = 10;
//                }

//                else
//                {
//                    if (holiday.Type == "Normal")
//                    {
//                        statusId = 12;
//                    }
//                    else
//                    {
//                        var leave = await _context.LeaveRequest.Where(x => x.EmpId == request.EmployeeId && x.FromDate == request.Date && x.status == 2).FirstOrDefaultAsync(cancellationToken);
//                        if (leave != null)
//                        {
//                            if (leave.TypeName == "WFH")
//                            {
//                                statusId = 8;
//                            }
//                            else
//                            {
//                                statusId = 12;
//                            }
//                        }
//                    }

//                }
//            }
//            else
//            {
//                var insideHrs = await _context.DailyAttendence.Where(x => x.EmployeeId == request.EmployeeId && x.Date == request.Date).Select(x => x.InsideDuration).FirstOrDefaultAsync(cancellationToken);
//                if (insideHrs < 360)
//                {
//                    statusId = 11;

//                }
//                else
//                {
//                    statusId = 9;
//                }
//            }

//            if (statusId == 11)
//            {
//                List<string> cCManagerEmail = new List<string>();
//                List<string> cCManagerName = new List<string>();

//                var employeeEmail = await _context.Employees.Where(x => x.EmployeeId == request.EmployeeId)
//               .Select(x => x.Email).FirstOrDefaultAsync();

//                var employeeName = await _context.Employees.Where(x => x.EmployeeId == request.EmployeeId)
//                    .Select(x => x.FirstName + " " + x.LastName).FirstOrDefaultAsync();

//                var managerId = await _context.Employees.Where(x => x.EmployeeId == request.EmployeeId)
//                    .Select(x => x.ManagerId).FirstOrDefaultAsync();

//                if (managerId != null)
//                {
//                    var managers = await FindManager(managerId, cancellationToken);
//                    cCManagerEmail.AddRange(managers.cCManagerEmail);
//                    cCManagerName.AddRange(managers.cCManagerName);
//                }

//                string Subject = "Attendence Low Warning";
//                string Message = $"{employeeName}'s working hour is less than required";
//                SendEmailCommand.SendMail(SenderEmail, SenderName, employeeEmail, employeeName, Subject, Message, cCManagerEmail.ToArray(), cCManagerName.ToArray());

//            }

//            Status status = new Status(request.EmployeeId, request.Date, statusId);

//            await _context.AddAsync(status);
//            return await _context.SaveChangesAsync();
//        }
//        private async Task<(List<string> cCManagerEmail, List<string> cCManagerName)> FindManager(int? managerId, CancellationToken cancellationToken)
//        {
//            List<string> cCManagerEmail = new List<string>();
//            List<string> cCManagerName = new List<string>();

//            var manager = await _context.Employees.Where(x => x.EmployeeId == managerId)
//                .Select(x => new
//                {
//                    x.Email,
//                    x.ManagerId,
//                    x.FirstName,
//                    x.LastName

//                }).FirstOrDefaultAsync();
//            if (manager != null)
//            {
//                cCManagerEmail.Add(manager.Email);
//                cCManagerName.Add(manager.FirstName + " " + manager.LastName);
//                if (manager.ManagerId != null)
//                {
//                    await FindManager(manager.ManagerId, cancellationToken);
//                }
//            }
//            return (cCManagerEmail, cCManagerName);
//        }

//    }

//}
