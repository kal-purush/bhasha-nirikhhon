using OrderService.Domain.Entities.OrderComponents;

namespace OrderService.Domain.Entities
{
    public class Order
    {
        public Guid Id { get; set; }
        public string OrderNumber { get; set; } = string.Empty;
        public Guid ClientId { get; set; }
        public Guid? CourierId { get; set; }
        public Status Status { get; set; } = new();
        public Address Address { get; set; } = new();
        public List<OrderMeal> Meals { get; set; } = new();
        public decimal TotalPrice { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public void CalculateTotalPrice()
        {
            TotalPrice = Meals.Sum(meal => meal.Price * meal.PortionsAmount);
        }
    }
}