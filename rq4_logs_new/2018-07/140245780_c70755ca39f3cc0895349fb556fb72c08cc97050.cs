using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using C2C2.Abstract;
using C2C2.Entities;
using C2C2.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using C2C2.Models;
using System.Security.Claims;
using C2C2.Models.ManageViewModels;
using C2C2.Services;


namespace C2C2.Concrete
{
    [Authorize]
    [Route("[controller]/[action]")]
    public class EFSubjectRepository: ISubjectRepository
    {
        
        private readonly SignInManager<ApplicationUser> _signInManager;
        public EFSubjectRepository(SignInManager<ApplicationUser> signInManager)
        {
            _signInManager = signInManager;
            
           
        }

        private readonly EFDbContext context = new EFDbContext();
        public IEnumerable<Subject> Subject
        {
            get { return context.Subject; }
        }

        public void SaveSubject(Subject subject, IndexViewModel model)
        {
            if (subject.SubjectId == 0)
            {
                context.Subject.Add(subject);
                context.Subject.Add(subject);
            }
            else
            {
                Subject dbEntry = context.Subject.Find(subject.SubjectId);
                if (dbEntry != null)
                {
                    var user = _signInManager.GetTwoFactorAuthenticationUserAsync();
                    dbEntry.SubjectName = subject.SubjectName;
                    dbEntry.CreatedDate = DateTime.Today;
                    dbEntry.UpdatedDate = DateTime.Today;
                    dbEntry.CreatedBy = model.Username;
                    
                }
            }
            context.SaveChanges();
        }


        public Subject DeleteSubject(int subjectId)
        {
            Subject dbEntry = context.Subject.Find(subjectId);
            if (dbEntry != null)
            {
                context.Subject.Remove(dbEntry);
                context.SaveChanges();
            }
            return dbEntry;
        }
    }
}