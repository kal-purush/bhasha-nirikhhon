using NerdStore.Sales.Application.Queries.ViewModels;
using NerdStore.Sales.Domain.Interfaces.Repositories;
using NerdStore.Sales.Domain.Models;
using NerdStore.Sales.Domain.Models.Enums;

namespace NerdStore.Sales.Application.Queries;

public class OrderQueries : IOrderQueries
{
    private readonly IOrderRepository _orderRepository;

    public OrderQueries(IOrderRepository orderRepository)
    {
        _orderRepository = orderRepository;
    }

    public async Task<CartViewModel> GetCustomerCartAsync(Guid customerId)
    {
        var order = await _orderRepository.GetOrderDraftByCustomerIdAsync(customerId);

        if (order is null) return null;

        var cart = new CartViewModel(order);

        if (order.VoucherId != null)
            cart.VoucherCode = order.Voucher.Code;

        foreach (var item in order.OrderItems)
            cart.Items.Add(new CartItemViewModel(item));

        return cart;
    }

    public async Task<IEnumerable<OrderViewModel>> GetCustomerOrdersAsync(Guid customerId)
    {
        var orders = await GetOrdersAsync(customerId);

        if (!orders.Any()) return null;

        var ordersViewModel = new List<OrderViewModel>();

        foreach (var order in orders)
            ordersViewModel.Add(new OrderViewModel(order));

        return ordersViewModel;
    }

    private async Task<IEnumerable<Order>> GetOrdersAsync(Guid customerId)
    {
        var permittedStatus = new List<OrderStatus>
        {
            OrderStatus.Paid,
            OrderStatus.Canceled
        };

        var orders = await _orderRepository.GetListByCustomerIdAsync(customerId);
        
        orders = orders
            .Where(o => permittedStatus.Contains(o.OrderStatus))
            .OrderByDescending(o => o.Code);

        return orders;
    }
}