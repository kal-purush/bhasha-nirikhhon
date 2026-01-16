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
    public class ImpOrderService : IImpOrderService
    {
        private readonly DrugRepositoryDBContext _context;
        public ImpOrderService(DrugRepositoryDBContext context)
        {
            _context = context;
        }
        public async Task<StatusView> CreateImpOrder(ImpOrderDto request)
        {
            Product product = await _context.Product.FindAsync(request.ProId);
            ImpOrder impOrder = new ImpOrder()
            {
                ImpId = request.ImpId,
                ImpDate = request.ImpDate,
                ImpNote = request.ImpNote,
                ImpTotal = product.ProPrice * request.Quantity,
                SupId = request.SupId,
                UserId = request.UserId
            };
            await _context.ImpOrder.AddAsync(impOrder);
            ImpOrderDetail impOrderDetail = new ImpOrderDetail()
            {
                ProId = request.ProId,
                Quantity = request.Quantity,
                ImpId = request.ImpId,
            };
            await _context.ImpOrderDetail.AddAsync(impOrderDetail);

            StoreProduct storeProduct = await _context.StoreProduct.FirstAsync(s => s.ProId == request.ProId);
            storeProduct.Quantity += request.Quantity;
            await _context.SaveChangesAsync();
            StatusView result = new StatusView()
            {
                status = "success",
                message = "import product order success !"
            };
            return result;
        }

        public async Task<List<ImpOrderView>> getAllImpOrder()
        {
            List<ImpOrderView> result = new List<ImpOrderView>();
            try
            {
                result = await _context.Set<ImpOrderView>().FromSqlRaw("sp_GetAll_ImpOrder").ToListAsync();
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}