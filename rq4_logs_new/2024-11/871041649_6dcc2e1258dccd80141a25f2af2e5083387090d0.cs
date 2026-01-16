using backend.Models;
using backend.Repositories.Interfaces;
using System;

namespace backend.Repositories.Implementations
{
    public class DeliveryRequestRepository : IDeliveryRequestRepository
    {
        private readonly FoodDeliveryDbContext _context;

        public DeliveryRequestRepository(FoodDeliveryDbContext context)
        {
            _context = context;
        }

        public void AddDeliveryRequest(DeliveryRequest deliveryRequest)
        {
            _context.DeliveryRequests.Add(deliveryRequest);
        }

        public void DeleteDeliveryRequest(int id)
        {
            var deliveryRequest = _context.DeliveryRequests.Find(id);
            if (deliveryRequest != null)
            {
                _context.DeliveryRequests.Remove(deliveryRequest);
            }
        }

        public int GetTotalDeliveryIncentiveByPartnerId(int deliveryPartnerId)
        {
            return _context.DeliveryRequests
                .Where(dr => dr.DeliveryPartnerId == deliveryPartnerId && dr.DeliveryInsentive.HasValue)
                .Sum(dr => dr.DeliveryInsentive.Value);
        }

        public List<int> GetOrderIdsByDeliveryPartnerId(int deliveryPartnerId)
        {
            return _context.DeliveryRequests
                .Where(dr => dr.DeliveryPartnerId == deliveryPartnerId)
                .Select(dr => dr.OrderId.Value)
                .ToList();
        }

        public void Save()
        {
            _context.SaveChanges();
        }

    }
}