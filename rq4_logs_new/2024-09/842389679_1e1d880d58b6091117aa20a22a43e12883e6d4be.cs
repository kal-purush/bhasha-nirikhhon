// using Application.Common.Models;
// using Application.Interfaces;
// using MediatR;
// using Microsoft.EntityFrameworkCore;
// using Domain.Entities;
//
// namespace Application.Modules.Queries
// {
//     public class GetStudentModuleDetailsQuery : IRequest<Result>
//     {
//         public int ModuleId { get; set; }
//         public string StudentId { get; set; }
//     }
//
//     public class GetStudentModuleByIdQueryHandler : IRequestHandler<GetStudentModuleDetailsQuery, Result>
//     {
//         private readonly IApplicationDbContext _context;
//
//         public GetStudentModuleByIdQueryHandler(IApplicationDbContext context)
//         {
//             _context = context;
//         }
//
//         public async Task<Result> Handle(GetStudentModuleDetailsQuery request, CancellationToken cancellationToken)
//         {
//             // Retrieve the module
//             Module? module = await _context.Modules
//                 .FirstOrDefaultAsync(m => m.Id == request.ModuleId, cancellationToken);
//
//             if (module == null)
//             {
//                 return Result.Failure($"Module with ID {request.ModuleId} not found.");
//             }
//
//             // Retrieve the corresponding student module
//             StudentModule? studentModule = await _context.StudentModules
//                 .FirstOrDefaultAsync(sm => sm.ModuleId == request.ModuleId && sm.StudentId == request.StudentId, cancellationToken);
//
//             if (studentModule == null)
//             {
//                 return Result.Failure($"StudentModule with ModuleId {request.ModuleId} and StudentId {request.StudentId} not found.");
//             }
//
//             // Combine module and student module into a result
//             var studentModuleDetails = new
//             {
//                 Module = module,
//                 StudentModule = studentModule
//             };
//
//             return Result.Success("Module and StudentModule retrieved successfully.", studentModuleDetails);
//         }
//     }
// }