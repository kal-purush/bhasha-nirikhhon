using BudgetTracker.BudgetSquirrel.Application.Messages;
using BudgetTracker.Business.Auth;
using Newtonsoft.Json;
using System;

namespace BudgetTracker.BudgetSquirrel.WebApi.Tests.ApiMessages
{
    public class ApiRequestHelper
    {
        public static ApiRequest ToMessage(UserRequestApiMessage credentials, object data)
        {
            string requestData = JsonConvert.SerializeObject(data);
            string messageStr = $"{{'user': {{ 'username': '{credentials.UserName}', 'password': '{credentials.Password}' }}," +
                                $"'arguments': {{ 'transaction-values': {requestData} }} }}";
            ApiRequest request = JsonConvert.DeserializeObject<ApiRequest>(messageStr);
            return request;
        }

        public static ApiRequest ToMessage(string username, string password, object data)
        {
            string requestData = JsonConvert.SerializeObject(data);
            string messageStr = $"{{'user': {{ 'username': '{username}', 'password': '{password}' }}," +
                                $"'arguments': {{ 'transaction-values': {requestData} }} }}";
            ApiRequest request = JsonConvert.DeserializeObject<ApiRequest>(messageStr);
            return request;
        }
    }
}