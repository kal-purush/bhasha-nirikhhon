using System.Configuration;
using System.Net.Http;
using apitool.core.Models.Criteria;
using apitool.core.Models.Parameters;

namespace apitool.core.Builders.Criteria
{
    public class ReservationStatusCriteriaBuilder : ICallCriteriaBuilder
    {
        public ICallCriteria Build(CallParameters parameters)
        {
            var url = string.Format("{0}.ashx/reservation/?st=0&pid={1}&rid={2}&conf={3}",
                           parameters.ApiVersion,
                           ConfigurationManager.AppSettings["PartnerId"],
                           parameters.RestaurantId,
                           parameters.ConfirmationNumber);

            return new CallCriteria
                {
                    CallParameters = new CallParameters
                        {
                            Url = url,
                            HttpMethod = HttpMethod.Get
                        }
                };
        }
    }
}