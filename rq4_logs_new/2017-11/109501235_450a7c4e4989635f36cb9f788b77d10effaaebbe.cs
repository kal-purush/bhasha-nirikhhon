using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlooringOrder.Model;
using FlooringOrder.Model.Response;
using FlooringOrder.Model.Interface;

namespace FlooringOrder.BLL
{
    public class OrderManager
    {
        private IOrderRepository _orderRepository;

        public OrderManager(IOrderRepository orderRepository)
        {
            _orderRepository = orderRepository;
        }

        public DisplayOrderResponse LookupOrder()
        {
            DisplayOrderResponse orderResponse = new DisplayOrderResponse();
            orderResponse.orders = _orderRepository.DisplayOrder();
            if(orderResponse.orders is null)
            {
                orderResponse.Success = false;
                //orderResponse.Message = $"{orderNumber} is not a valid order number.";
                orderResponse.Message = "Cannot load order.";
            }
            else
            {
                orderResponse.Success = true;
            }
            return orderResponse;

        }

        public bool IfFileExist()
        {
            return _orderRepository.IfFileExist();
        }

        public AddOrderResponse AddOrder(Orders order)
        {
            DisplayOrderResponse orderResponse = new DisplayOrderResponse();
            orderResponse.orders = _orderRepository.DisplayOrder();

            AddOrderResponse addResponse = new AddOrderResponse();
            addResponse.orders = order;
            if (orderResponse.orders != null && orderResponse.orders.Where(c=>c.OrderNumber == order.OrderNumber).Count() > 0)
            {
                addResponse.Success = false;
                addResponse.Message = $"{order.OrderNumber} is already existed.";
                return addResponse;
            }

            if (!_orderRepository.AddOrder(order))
            {
                addResponse.Success = false;
                addResponse.Message = $"Cannot add {order.OrderNumber}.";                
            }
            else
            {
                addResponse.Success = true;
                addResponse.Message = $"Successfully added order {order.OrderNumber}.";
            }
            return addResponse;
        }

        public EditOrderResponse EditOrder(Orders order)
        {
            EditOrderResponse editResponse = new EditOrderResponse();
            if (!_orderRepository.EditOrder(order))
            {
                editResponse.Success = false;
                editResponse.Message = $"Cannot edit {order.OrderNumber}.";
            }
            else
            {
                editResponse.Success = true;
                editResponse.Message = $"Successfully edited order {order.OrderNumber}.";
            }
            return editResponse;
        }

        public RemoveOrderResponse RemoveOrder(Orders order)
        {
            RemoveOrderResponse removeResponse = new RemoveOrderResponse();
            if (!_orderRepository.RemoveOrder(order))
            {
                removeResponse.Success = false;
                removeResponse.Message = $"Cannot remove {order.OrderNumber}.";
            }
            else
            {
                removeResponse.Success = true;
                removeResponse.Message = $"Successfully removed order {order.OrderNumber}.";
            }
            return removeResponse;
        }

        public int GenerateOrderNumber()
        {
            return _orderRepository.GetOrderNumber();
        }

        public Orders GetOrder(int orderNumber)
        {
            return _orderRepository.GetOrder(orderNumber);
        }

    }
}