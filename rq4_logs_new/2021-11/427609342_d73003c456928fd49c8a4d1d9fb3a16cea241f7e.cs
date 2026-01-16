using Application.IServices;
using Data.Dtos;
using Data.Models;
using Data.ViewModels;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Application.Services
{
    public class SupplierService : ISupplierService
    {
        private readonly DrugRepositoryDBContext _context;
        public SupplierService(DrugRepositoryDBContext context)
        {
            _context = context;
        }
        public async Task<StatusView> deleteSupplier(string supId)
        {
            StatusView result = new StatusView()
            {
                status = "false",
                message = "delete supplier failed !"
            };
            if (supId == null)
            {
                return result;
            }
            var supplier = _context.Supplier.Find(supId);
            _context.Supplier.Remove(supplier);
            await _context.SaveChangesAsync();
            result.status = "true";
            result.message = "delete success";
            return result;
        }

        public async Task<List<SupplierView>> getAllSupplier()
        {
            List<SupplierView> result = new List<SupplierView>();
            try
            {
                result = await _context.Set<SupplierView>().FromSqlRaw("sp_GetAll_Supplier").ToListAsync();
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<StatusView> updateOrCreateSupplier(SupplierDto request)
        {
            StatusView result = new StatusView()
            {
                status = "false",
                message = "update or create supplier failed !"
            };
            if (request == null)
            {
                return result;
            }
            var temp = await _context.Database.ExecuteSqlRawAsync($"sp_AddOrUpdate_Supplier {request.SupId}, {request.SupName}, {request.SupEmail}, {request.SupPhone}, {request.SupAddress}, {request.SupNo}, {request.SupLicense}");
            if (temp == 1)
            {
                result.status = "true";
                result.message = "update or create supplier success";
                return result;
            }
            return result;
        }
    }
}