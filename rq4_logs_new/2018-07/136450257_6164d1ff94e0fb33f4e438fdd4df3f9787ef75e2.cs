using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using NCS.DSS.Goal.Annotations;
using NCS.DSS.Goal.Cosmos.Helper;
using NCS.DSS.Goal.GetGoalByIdHttpTrigger.Service;
using NCS.DSS.Goal.Helpers;
using NCS.DSS.Goal.Ioc;
using Newtonsoft.Json;

namespace NCS.DSS.Goal.GetGoalByIdHttpTrigger.Function
{
    public static class GetGoalByIdHttpTrigger
    {
        [FunctionName("GetById")]
        [Response(HttpStatusCode = (int)HttpStatusCode.OK, Description = "Goal found", ShowSchema = true)]
        [Response(HttpStatusCode = (int)HttpStatusCode.NoContent, Description = "Goal does not exist", ShowSchema = false)]
        [Response(HttpStatusCode = (int)HttpStatusCode.BadRequest, Description = "Request was malformed", ShowSchema = false)]
        [Response(HttpStatusCode = (int)HttpStatusCode.Unauthorized, Description = "API key is unknown or invalid", ShowSchema = false)]
        [Response(HttpStatusCode = (int)HttpStatusCode.Forbidden, Description = "Insufficient access", ShowSchema = false)]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Customers/{customerId}/Interactions/{interactionId}/ActionPlans/{actionPlanId}/Goals/{goalId}")]HttpRequestMessage req, ILogger log, string customerId, string interactionId, string actionPlanId, string goalId,
            [Inject]IResourceHelper resourceHelper,
            [Inject]IGetGoalByIdHttpTriggerService goalGetService)
        {
            log.LogInformation("Get Goal By Id C# HTTP trigger function  processed a request.");

            if (!Guid.TryParse(customerId, out var customerGuid))
                return HttpResponseMessageHelper.BadRequest(customerGuid);

            if (!Guid.TryParse(interactionId, out var interactionGuid))
                return HttpResponseMessageHelper.BadRequest(interactionGuid);

            if (!Guid.TryParse(actionPlanId, out var actionPlanGuid))
                return HttpResponseMessageHelper.BadRequest(actionPlanGuid);

            if (!Guid.TryParse(goalId, out var goalGuid))
                return HttpResponseMessageHelper.BadRequest(goalGuid);

            var doesCustomerExist = resourceHelper.DoesCustomerExist(customerGuid);

            if (!doesCustomerExist)
                return HttpResponseMessageHelper.NoContent(customerGuid);

            var doesInteractionExist = resourceHelper.DoesInteractionExist(interactionGuid);

            if (!doesInteractionExist)
                return HttpResponseMessageHelper.NoContent(interactionGuid);

            var doesActionPlanExist = resourceHelper.DoesActionPlanExist(actionPlanGuid);

            if (!doesActionPlanExist)
                return HttpResponseMessageHelper.NoContent(actionPlanGuid);

            var action = await goalGetService.GetGoalForCustomerAsync(customerGuid, goalGuid);

            return action == null ?
                HttpResponseMessageHelper.NoContent(customerGuid) :
                HttpResponseMessageHelper.Ok(action);

        }
    }
}