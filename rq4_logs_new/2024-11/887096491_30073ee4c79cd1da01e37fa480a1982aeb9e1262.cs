////using LanguageExt.ClassInstances.Pred;
////using MediatR;
////using Microsoft.EntityFrameworkCore;
////using System;
////using System.Collections.Generic;
////using System.Linq;
////using System.Text;
////using System.Threading.Tasks;
////using WolfDen.Application.DTOs.LeaveManagement;
////using WolfDen.Domain.Entity;
////using WolfDen.Domain.Enums;
////using WolfDen.Infrastructure.Data;

////namespace WolfDen.Application.Requests.Commands.LeaveManagement.LeaveBalances.UpdateLeaveBalance
////{
////    public class UpdateLeaveBalanceCommandHandler : IRequestHandler<UpdateLeaveBalanceCommand, bool>
////    {
////        private readonly WolfDenContext _context;

////        public UpdateLeaveBalanceCommandHandler(WolfDenContext context)
////        {
////            _context = context;
////        }
////        public async Task<bool> Handle(UpdateLeaveBalanceCommand request, CancellationToken cancellationToken)
////        {
////            foreach (Employee emp in _context.Employees)
////            {
////                // List<LeaveType> leaveTypes= _context.LeaveType.ToList();
////                List<LeaveBalance> leaveBalance = await _context.LeaveBalances.Where(x => x.EmployeeId == emp.Id).ToListAsync(cancellationToken: cancellationToken);
////                LeaveSetting leaveSetting = await _context.LeaveSettings.FirstOrDefaultAsync(cancellationToken);

////                foreach (LeaveType leaveType in _context.LeaveType)
////                {
////                    foreach (LeaveBalance leaveBalance1 in leaveBalance)
////                    {
////                        LeaveIncrementLog leaveIncrementLog = await _context.LeaveIncrementLogs.Where(x => x.LeaveBalanceId == leaveBalance1.Id).FirstOrDefaultAsync(cancellationToken);//check whethre same user is passed of details ALSO last log date is to be got not the first

////                        if (emp.JoiningDate.Value.Month == DateTime.Now.Month && emp.JoiningDate.Value.Year == DateTime.Now.Year)
////                        {
////                            if (leaveSetting.MinDaysForLeaveCreditJoining < emp.JoiningDate.Value.Day)
////                            {
////                                leaveBalance1.Balance = 1;
////                            }
////                            else
////                            {
////                                leaveBalance1.Balance = 0;
////                            }
////                        }
////                        else
////                        {
////                            DateTime JoiningDateTime = emp.JoiningDate.Value.ToDateTime(TimeOnly.MinValue);
////                            if (DateTime.Now.Subtract(JoiningDateTime).Days >= leaveType.DutyDaysRequired)
////                            {
////                                DateTime incrementLog = leaveIncrementLog.LogDate.ToDateTime(TimeOnly.MinValue);
////                                int UpdateReq = DateTime.Now.Subtract(incrementLog).Days;  //check whether the updations is to be done or not depending on incrementgaptypes conditions

////                                //check whether the applied date is more than the increment gap mentioned


////                                if (LeaveIncrementGapMonth.Month == leaveType.IncrementGap)   //monthly increment
////                                {
////                                    int value = UpdateReq / 1;
////                                    if (leaveType.CarryForward == true)
////                                    {
////                                        if (leaveBalance1.Balance + (leaveType.IncrementCount * value) > leaveType.CarryForwardLimit)
////                                        {
////                                            leaveBalance1.Balance = (decimal)leaveType.CarryForwardLimit;     //if new balance exceeds carrylimit
////                                        }
////                                        else
////                                        {
////                                            leaveBalance1.Balance += (decimal)leaveType.IncrementCount * value;
////                                        }
////                                    }
////                                    else //if no carry forward allowed
////                                    {
////                                        if (DateTime.Now.Year == leaveIncrementLog.LogDate.Year)   //check if the current date Year is same as last log dateYearb if yes then update accordingly
////                                        {
////                                            leaveBalance1.Balance += (decimal)leaveType.IncrementCount * value;
////                                        }
////                                        else    //if balance updated in next/new year
////                                        {
////                                            if (leaveBalance1.Balance + (leaveType.IncrementCount * value) > leaveType.CarryForwardLimit)
////                                            {
////                                                //leaveBalanceDto.Balance = (decimal)leaveType.CarryForwardLimit;
////                                            }
////                                            else
////                                            {
////                                                leaveBalance1.Balance += (decimal)leaveType.IncrementCount * value;
////                                            }
////                                        }
////                                    }
////                                    LeaveIncrementLog leaveIncrement = new LeaveIncrementLog(leaveBalance1.Id, DateOnly.FromDateTime(DateTime.Now), leaveBalance1.Balance, (int)(leaveType.IncrementCount * value));
////                                    _context.LeaveIncrementLogs.Add(leaveIncrement);
////                                    await _context.SaveChangesAsync();
////                                }


////                                else if (LeaveIncrementGapMonth.Quarter == leaveType.IncrementGap)    //quarterly increment
////                                {
////                                    int value = UpdateReq / 3;
////                                    if (leaveType.CarryForward == true)
////                                    {
////                                        if (leaveBalance1.Balance + (leaveType.IncrementCount * value) > leaveType.CarryForwardLimit)
////                                        {
////                                            leaveBalance1.Balance = (decimal)leaveType.CarryForwardLimit;
////                                        }
////                                        else
////                                        {
////                                            leaveBalance1.Balance += (decimal)leaveType.IncrementCount * value;
////                                        }
////                                    }
////                                }

////                                else if (LeaveIncrementGapMonth.Half == leaveType.IncrementGap)  //half yearly increment
////                                {
////                                    int value = UpdateReq / 6;
////                                    if (leaveType.CarryForward == true)
////                                    {
////                                        if (leaveBalance1.Balance + (leaveType.IncrementCount * value) > leaveType.CarryForwardLimit)
////                                        {
////                                            leaveBalance1.Balance = (decimal)leaveType.CarryForwardLimit;
////                                        }
////                                        else
////                                        {
////                                            leaveBalance1.Balance += (decimal)leaveType.IncrementCount * value;
////                                        }
////                                    }
////                                }

////                            }
////                            //update increment log table with the current date  
////                        }
////                    }
////                }
////            }
////            return true;
////        }

////    }
////}






//using MediatR;
//using Microsoft.EntityFrameworkCore;
//using WolfDen.Application.Requests.Commands.LeaveManagement.LeaveBalances.UpdateLeaveBalance;
//using WolfDen.Domain.Entity;
//using WolfDen.Domain.Enums;
//using WolfDen.Infrastructure.Data;

//public class UpdateLeaveBalanceCommandHandler(WolfDenContext context) : IRequestHandler<UpdateLeaveBalanceCommand, bool>
//{
//    private readonly WolfDenContext _context = context;

//    public async Task<bool> Handle(UpdateLeaveBalanceCommand request, CancellationToken cancellationToken)
//    {
//        // First, fetch all leave balances and leave settings
//        var leaveBalances = await _context.LeaveBalances
//            .Where(x => x.EmployeeId != null) // Only fetch for existing employees
//            .ToListAsync(cancellationToken);

//        var leaveTypes = await _context.LeaveType.ToListAsync(cancellationToken);
//        var leaveSetting = await _context.LeaveSettings.FirstOrDefaultAsync(cancellationToken);
//        var leaveIncrementLogs = await _context.LeaveIncrementLogs.ToListAsync(cancellationToken);  // Fetch once

//        // Fetch all employees
//        var employees = await _context.Employees.ToListAsync(cancellationToken);

//        foreach (var emp in employees)
//        {
//            // Filter leave balances for the current employee
//            var empLeaveBalances = leaveBalances.Where(x => x.EmployeeId == emp.Id).ToList();
//            var employeeIncrementLogs = leaveIncrementLogs.Where(log => empLeaveBalances.Any(lb => lb.Id == log.LeaveBalanceId)).ToList();

//            foreach (var leaveBalance in empLeaveBalances)
//            {
//                // Find the related leave increment log for this leave balance
//                var leaveIncrementLog = employeeIncrementLogs
//                    .FirstOrDefault(log => log.LeaveBalanceId == leaveBalance.Id);

//                foreach (var leaveType in leaveTypes)
//                {
//                    if (emp.JoiningDate.Value.Month == DateTime.Now.Month && emp.JoiningDate.Value.Year == DateTime.Now.Year)
//                    {
//                        if (leaveSetting.MinDaysForLeaveCreditJoining < emp.JoiningDate.Value.Day)
//                        {
//                            leaveBalance.Balance = 1;
//                        }
//                        else
//                        {
//                            leaveBalance.Balance = 0;
//                        }
//                    }
//                    else
//                    {
//                        DateTime joiningDateTime = emp.JoiningDate.Value.ToDateTime(TimeOnly.MinValue);
//                        if (DateTime.Now.Subtract(joiningDateTime).Days >= leaveType.DutyDaysRequired)
//                        {
//                            //  DateTime incrementLog = leaveIncrementLog?.LogDate.ToDateTime(TimeOnly.MinValue) ?? DateTime.MinValue;
//                            DateTime incrementLog = leaveIncrementLog?.LogDate.HasValue == true
//      ? leaveIncrementLog.LogDate.Value.ToDateTime(TimeOnly.MinValue)
//      : DateTime.MinValue;

//                            int updateReq = DateTime.Now.Subtract(incrementLog).Days;
//                            int value = 0;

//                            // Apply increment gap logic based on LeaveType
//                            if (LeaveIncrementGapMonth.Month == leaveType.IncrementGap) // Monthly increment
//                            {
//                                value = updateReq / 1;
//                                leaveBalance.Balance = await ApplyLeaveIncrement(leaveBalance, leaveType, value);
//                            }
//                            else if (LeaveIncrementGapMonth.Quarter == leaveType.IncrementGap) // Quarterly increment
//                            {
//                                value = updateReq / 3;
//                                leaveBalance.Balance = await ApplyLeaveIncrement(leaveBalance, leaveType, value);
//                            }
//                            else if (LeaveIncrementGapMonth.Half == leaveType.IncrementGap) // Half-yearly increment
//                            {
//                                value = updateReq / 6;
//                                leaveBalance.Balance = await ApplyLeaveIncrement(leaveBalance, leaveType, value);
//                            }

//                            // Save the increment log after applying changes
//                            LeaveIncrementLog leaveIncrement = new LeaveIncrementLog(
//                                leaveBalance.Id,
//                                DateOnly.FromDateTime(DateTime.Now),
//                                leaveBalance.Balance,
//                                (int)(leaveType.IncrementCount * value)
//                            );
//                            _context.LeaveIncrementLogs.Add(leaveIncrement);
//                        }
//                    }
//                }
//            }
//        }

//        // Save changes at once after the loop
//        await _context.SaveChangesAsync(cancellationToken);

//        return true;
//    }

//    private async Task<decimal> ApplyLeaveIncrement(LeaveBalance leaveBalance, LeaveType leaveType, int value)
//    {
//        decimal newBalance = leaveBalance.Balance + (decimal)(leaveType.IncrementCount * value);

//        if ((bool)leaveType.CarryForward)
//        {
//            if (newBalance > leaveType.CarryForwardLimit)
//            {
//                return (decimal)leaveType.CarryForwardLimit; // If new balance exceeds the carry-forward limit
//            }
//        }

//        return newBalance;
//    }
//}

