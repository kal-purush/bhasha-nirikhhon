using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using C2C2.Data;
using C2C2.Models.QuestionViewModels;
using C2C2.Services;
using C2C2.Entities;

namespace C2C2.Concrete
{
    public class EFDbContext: DbContext
    {
        public DbSet<Question> Question { get; set; }
       
    }
}