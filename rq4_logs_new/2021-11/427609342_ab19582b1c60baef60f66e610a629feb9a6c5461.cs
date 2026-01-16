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
    public class ExpOrderService : IExpOrderService
    {
        private readonly DrugRepositoryDBContext _context;
        public ExpOrderService(DrugRepositoryDBContext context)
        {
            _context = context;
        }
        public async Task<StatusView> createExpOrder(ExpOrderDto request)
        {
            ExpOrder expOrder = new ExpOrder()
            {
                ExpId = request.ExpId,
                ExpDate = request.ExpDate,
                ExpNote = request.ExpNote,
                CusId = request.CusId,
                ExpStatus = 1,
                UserId = request.UserId,
                ExpTotal = request.ExpPrice * request.Quantity
            };
            await _context.ExpOrder.AddAsync(expOrder);
            ExpOrderDetail expOrderDetail = new ExpOrderDetail()
            {
                ExpId = request.ExpId,
                ExpPrice = request.ExpPrice,
                ProId = request.ProId,
                Quantity = request.Quantity,
                ProNote = "1"
            };
            await _context.ExpOrderDetail.AddAsync(expOrderDetail);

            StoreProduct storeProduct = await _context.StoreProduct.FirstAsync(s => s.ProId == request.ProId);
            if(storeProduct.Quantity < request.Quantity)
            {
                StatusView res = new StatusView()
                {
                    status = "failed",
                    message = "store is out of stock"
                };
                return res;
            }
            storeProduct.Quantity -= request.Quantity;
            await _context.SaveChangesAsync();
            StatusView result = new StatusView()
            {
                status = "success",
                message = "import product order success !"
            };
            return result;
        }

        public async Task<List<ExpOrderView>> getAllExpOrder()
        {
            List<ExpOrderView> result = new List<ExpOrderView>();
            try
            {
                result = await _context.Set<ExpOrderView>().FromSqlRaw("sp_GetAll_ExpOrder").ToListAsync();
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}