using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using HaloBiz.Data;
using HaloBiz.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace HaloBiz.Repository.Impl
{
    public class ServicesRepositoryImpl : IServicesRepository
    {
        private readonly DataContext _context;
        private readonly ILogger<ServicesRepositoryImpl> _logger;

        public ServicesRepositoryImpl(DataContext context, ILogger<ServicesRepositoryImpl> logger)
        {
            this._context = context;
            this._logger = logger;
        }

         public async Task<Services> SaveService(Services service)
        {
            var savedEntity = await _context.Services.AddAsync(service);
            if(await SaveChanges())
            {
                return savedEntity.Entity;
            }
            return null;
        }

        public async Task<Services> FindServicesById(long Id)
        {
            return await _context.Services
                .FirstOrDefaultAsync( service => service.Id == Id);
        }

        public async Task<Services> FindServiceByName(string name)
        {
            return await _context.Services
                .FirstOrDefaultAsync( service => service.Name == name);
        }

        public async Task<IEnumerable<Services>> FindAllServices()
        {
            return await _context.Services
                .ToListAsync();
        }

        public async Task<Services> UpdateServices(Services service)
        {
            var updatedEntity =  _context.Services.Update(service);
            if(await SaveChanges())
            {
                return updatedEntity.Entity;
            }
            return null;
        }

        public async Task<bool> DeleteService(Services service)
        {
            _context.Services.Remove(service);
            return await SaveChanges();
        }

        private async Task<bool> SaveChanges()
        {
           try{
               return  await _context.SaveChangesAsync() > 0;
           }catch(Exception ex)
           {
               _logger.LogError(ex.Message);
               return false;
           }
        }

    }
}