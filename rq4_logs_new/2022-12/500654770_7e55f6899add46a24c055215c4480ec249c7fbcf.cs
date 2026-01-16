namespace Transenvios.Shipping.Api.Domains.ShipmentOrderService.ShipmentOrderPage
{
    public enum PaymentStates
    {
        UnPaid,
        Paid
    }

    public enum ShipmentStates
    {
        None,
        Collecting,
        InWarehouse,
        OnRoute,
        Delivered
    }
}