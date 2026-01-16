using Pri.CleanArchitecture.Core.Entities;
using Pri.CleanArchitecture.Core.Services.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pri.CleanArchitecture.Core.Interfaces.Services
{
    public interface ICategoryService
    {
        Task<ResultModel<Category>> GetById(int id);
    }
}