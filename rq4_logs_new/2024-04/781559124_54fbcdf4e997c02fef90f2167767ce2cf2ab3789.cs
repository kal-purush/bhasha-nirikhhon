using RMS.Application.Common.Interfaces;
using RMS.Application.Common.Interfaces.Repositories;
using RMS.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RMS.Application.Services;

public class OrderService(IBaseRepository<Order> orderRepository) : IBaseService<Order>
{
    public async Task<Order> CreateAsync(Order order, CancellationToken token = default)
    {
        return await orderRepository.CreateAsync(order, token);
    }

    public async Task<bool> DeleteAsync(int id, CancellationToken token = default)
    {
        var order = await orderRepository.GetAsync(id, token);

        if(order == null)
        {
            return false;
        }
        return await orderRepository.DeleteAsync(order, token);
    }

    public async Task<IEnumerable<Order>> GetAllAsync(CancellationToken token = default)
    {
        return await orderRepository.GetAllAsync(token);
    }

    public async Task<Order> GetAsync(int id, CancellationToken token = default)
    {
        return await orderRepository.GetAsync(id, token);   
    }

    public async Task<bool> UpdateAsync(Order order, CancellationToken token = default)
    {
        var orderExist = await orderRepository.GetAsync(order.Id, token);

        if (orderExist == null)
        {
            return false;
        }

        return await orderRepository.UpdateAsync(order, token);
    }
}