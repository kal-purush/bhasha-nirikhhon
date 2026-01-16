using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace XabluApp.Core
{
    public class EmployeeClient : IEmployeeClient
    {
        public EmployeeClient()
        {
            SetEmployeeData();
        }

        public async Task<EmployeeModel> GetEmployee(int Id)
        {
            return await Task.FromResult(Employees.FirstOrDefault(e => e.Id == Id));
        }

        public async Task<IEnumerable<EmployeeModel>> GetAllEmployees()
        {
            return await Task.FromResult(Employees);
        }

        #region Mock Data
        private List<EmployeeModel> Employees = new List<EmployeeModel>();
        public void SetEmployeeData()
        {
            int id = 0;
            Employees = new List<EmployeeModel>()
            {
                new EmployeeModel(){ Id = id++, Name = "Eric Slaager", Position = "CEO & Co-Founder of Xablu", Email = "", LinkedIn = "https://nl.linkedin.com/in/ericslaager"},
                new EmployeeModel(){ Id = id++, Name = "Martijn van Dijk", Position = "Xamarin Consultant @ Xablu", LinkedIn = "https://nl.linkedin.com/in/martijn-van-dijk"},
                new EmployeeModel(){ Id = id++, Name = "Marc Bruins", Position = "Xamarin Consultant at Xablu", LinkedIn = "https://nl.linkedin.com/in/marc-bruins-86819228"},
                new EmployeeModel(){ Id = id++, Name = "Harry ten Berge", Position = "Operations Manager at Xablu", LinkedIn = "https://nl.linkedin.com/in/harrytenberge"},
                new EmployeeModel(){ Id = id++, Name = "Coen Wessels", Position = "Software engineer & Xamarin Consultant bij Xablu", LinkedIn = "https://nl.linkedin.com/in/cowessels/en"},
                new EmployeeModel(){ Id = id++, Name = "Tirza van Dijk", Position = "Tirza van Dijk", LinkedIn = "Tirza van Dijk"},
                new EmployeeModel(){ Id = id++, Name = "Edwin Van der Ham", Position = "Senior Software Developer at Xablu", LinkedIn = "https://nl.linkedin.com/in/edwin-van-der-ham-6904075"},
                new EmployeeModel(){ Id = id++, Name = "Maurits van Beusekom", Position = "Senior Xamarin Developer at Xablu", LinkedIn = "https://nl.linkedin.com/in/maurits-van-beusekom-a3637958"},
            };
        }
        #endregion
    }
}