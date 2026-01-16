namespace DontRepeatYourself;

public static class OrderCalculator
{
  public static decimal CalculateTotal(Order order) 
  {
    decimal total = 0;

    foreach (var item in order.Items)
    {
      total += item.Price * item.Quantity;
    }

    total -= order.Discount;

    return total;
  }
}