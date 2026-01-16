using AutoMapper;
using OnlineStore.Domain.Common.Responses;
using OnlineStore.Domain.Constants;
using OnlineStore.Domain.Entities;
using OnlineStore.Infrastructure.Interfeces;
using OnlineStore.Service.DTOs.Order;
using OnlineStore.Service.DTOs.Order;
using OnlineStore.Service.Interfaces;

namespace OnlineStore.Service.Services
{
    public class OrderService : IOrderService
    {
        private readonly IUnitOfWork unitOfWork;
        private readonly IMapper mapper;

        public OrderService(IUnitOfWork unitOfWork, IMapper mapper)
        {
            this.unitOfWork = unitOfWork;
            this.mapper = mapper;
        }

        public async Task<Order> AddOrderAsync(OrderForCreationDto orderDto)
        {
            try
            {
                if (orderDto.CustomerId == default || orderDto.ProductId == null)
                {
                    throw new ErrorCodeException(ResponseMessages.ERROR_INVALID_DATA);
                }

                var existCustomer = (await unitOfWork.Customers.GetAllAsync())
                    .Any(customer => customer.Id == orderDto.CustomerId);
                var existOrder = (await unitOfWork.Orders.GetAllAsync())
                    .Any(order => order.Id == orderDto.ProductId);
                if (!existCustomer || !existOrder)
                {
                    throw new ErrorCodeException(ResponseMessages.ERROR_NOT_FOUND_DATA);
                }

                var priceForOrder = (await unitOfWork.Products.GetAsync(order => order.Id == orderDto.ProductId)).Price;
                var order = mapper.Map<Order>(orderDto);
                order.TotalPrice = orderDto.Count * priceForOrder;
                var result = await unitOfWork.Orders.CreateAsync(order);
                if (result != null)
                {
                    return result;
                }
                throw new ErrorCodeException(ResponseMessages.ERROR_ADD_DATA);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<IQueryable<Order>> RetrieveAllOrders()
        {
            try
            {
                var result = await unitOfWork.Orders.GetAllAsync();
                return result;
            }
            catch (Exception exception)
            {
                throw;
            }
        }

        public async Task<Order> RetrieveOrderByIdAsync(int orderId)
        {
            try
            {
                Order order = await unitOfWork.Orders.GetAsync(order => order.Id == orderId);
                return order;
            }
            catch (Exception exception)
            {
                throw;
            }
        }

        public async Task<Order> ModifyOrderAsync(int orderId,OrderForUpdateDto orderDto)
        {
            try
            {
                if (orderId == null)
                {
                    throw new ErrorCodeException(ResponseMessages.ERROR_INVALID_DATA);
                }

                var order = await unitOfWork.Orders.GetAsync(order => order.Id == orderId);
                if (order == null)
                {
                    throw new ErrorCodeException(ResponseMessages.ERROR_NOT_FOUND_DATA);
                }

                order.CreatedDate = orderDto.CreatedDate;
                order.UpdatedDate = orderDto.UpdatedDate;
                
                var result = await unitOfWork.Orders.UpdateAsync(order);
                await unitOfWork.SaveChangesAsync();

                if (result == null)
                {
                    throw new ErrorCodeException(ResponseMessages.ERROR_EDIT_DATA);
                }

                return result;
            }
            catch (Exception excepion)
            {
                throw;
            }
        }

        public async Task<bool> RemoveOrderByIdAsync(int orderId)
        {
            try
            {
                var order = await unitOfWork.Orders.GetAsync(order => order.Id == orderId);
                if (order == null)
                {
                    throw new ErrorCodeException(ResponseMessages.ERROR_NOT_FOUND_DATA);
                }

                await unitOfWork.Orders.DeleteAsync(order => order.Id == orderId);
                await unitOfWork.SaveChangesAsync();
                return true;
            }
            catch (Exception exception)
            {
                throw;
            }
        }
    }
}