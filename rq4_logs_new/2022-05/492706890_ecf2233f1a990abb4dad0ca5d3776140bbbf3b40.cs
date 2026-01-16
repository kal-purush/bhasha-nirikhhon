using PsicopataPedidos.Domain.Models;

namespace PsicopataPedidos.Application.OrderServices
{
    public class OrderService :  IOrderService
    {
        private readonly IOrderRepository _orderRepository;
        public OrderService(IOrderRepository orderRepository)
        {
            _orderRepository = orderRepository;
        }

        public Order CreateOrder(Order order)
        {
            _orderRepository.CreateOrder(order);
            return order;
        }



        public Order DeleteOrder(Order order)
        {
            _orderRepository.DeleteOrder(order);
            return order;
        }

        public List<Order> GetAllOrders()
        {
            var order = _orderRepository.GetAllOrders();
            return order;
        }
    }
}