using DS.Handlers.Abstract;

namespace DS.Handlers.Requests
{
    public class GetPossibleDeliveryRoutesHandlerRequest : HandlerRequestBase
    {
        public string Route { get; }
        public int MaxRouteRepeat { get; }
        public int? MaxDeliveryCost { get; }
        public int? MaxStop { get; }

        public GetPossibleDeliveryRoutesHandlerRequest(string route, int maxRouteRepeat, int? maxDeliveryCost, int? maxStop)
        {
            Route = route;
            MaxRouteRepeat = maxRouteRepeat;
            MaxDeliveryCost = maxDeliveryCost;
            MaxStop = maxStop;
        }
    }
}