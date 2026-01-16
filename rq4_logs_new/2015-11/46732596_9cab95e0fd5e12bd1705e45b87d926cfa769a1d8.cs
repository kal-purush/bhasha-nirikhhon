using System;
using System.Collections.Generic;
using System.Linq;

namespace ShippingApplication
{
    public class ShippingCalculator
    {
        public double? CalculateShippingCost(Order order)
        {
            List<ShippingMethod> methods = GetShippingRates();
            double? result = null;

            try
            {
                if (methods != null && methods.Count > 0)
                {
                    ShippingMethod method = methods.FirstOrDefault(t => t.MethodName == order.ShippingMethod);

                    if (method != null)
                    {
                        if (order.TravelDistance <= 100)
                        {
                            result = (order.Weight*method.ShippingRateClose) + method.HandlingFees;
                        }
                        else if (order.TravelDistance > 100 && order.TravelDistance <= 500)
                        {
                            result = (order.Weight*method.ShippingRateFarther) + method.HandlingFees;
                        }
                        else if (order.TravelDistance > 500)
                        {
                            result = (order.Weight*method.ShippingRateFarthest) + method.HandlingFees;
                        }
                        else
                        {
                            throw new Exception("Shipping Method could not be calculated");
                        }
                    }
                    else
                    {
                        throw new Exception("Shipping Method could not be calculated");
                    }
                }
                else
                {
                    throw new Exception("Shipping Method could not be calculated");
                }
            }
            catch (Exception ex)
            {
                LogException(ex);
            }

            return result;
        }

        protected List<ShippingMethod> GetShippingRates()
        {
            List<ShippingMethod> methods = new List<ShippingMethod>
            {
                new ShippingMethod
                {
                    MethodName = "UPS",
                    ShippingRateClose = 1.23,
                    ShippingRateFarther = 1.89,
                    ShippingRateFarthest = 2.60,
                    HandlingFees = 5.00
                },
                new ShippingMethod
                {
                    MethodName = "FedEx",
                    ShippingRateClose = 1.30,
                    ShippingRateFarther = 2.00,
                    ShippingRateFarthest = 2.50,
                    HandlingFees = 6.00
                },
                new ShippingMethod
                {
                    MethodName = "USPS",
                    ShippingRateClose = 1.34,
                    ShippingRateFarther = 1.85,
                    ShippingRateFarthest = 2.97,
                    HandlingFees = 4.50
                },
                new ShippingMethod
                {
                    MethodName = "DHL",
                    ShippingRateClose = 1.35,
                    ShippingRateFarther = 2.10,
                    ShippingRateFarthest = 3.00,
                    HandlingFees = 3.89
                }
            };

            return methods;
        }
    }

    public class Order
    {
        public int OrderNumber { get; set; }
        public string ShippingMethod { get; set; }
        public double Weight { get; set; }
        public int TravelDistance { get; set; }
    }

    public class ShippingMethod
    {
        public string MethodName { get; set; }
        public double ShippingRateClose { get; set; }
        public double ShippingRateFarther { get; set; }
        public double ShippingRateFarthest { get; set; }
        public double HandlingFees { get; set; }
    }
}