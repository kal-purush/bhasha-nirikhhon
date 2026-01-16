using JobBoard.Application.Command.Abstraction;
using JobBoard.Database.Repository.Abstraction;
using JobBoard.Model.EmployeeResume;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobBoard.Application.Command
{
    public class GetEmployeeResumeCountCommand : GetEmployeeResumeRequest, IGetCountCommand
    {
    }

    public class GetEmployeeResumeCountHandler : GetCountHandler<EmployeeResumeModel, GetEmployeeResumeRequest, GetEmployeeResumeCountCommand>
    {
        public GetEmployeeResumeCountHandler(IEmployeeResumeRepository repository) : base(repository)
        {
            
        }
    }
}