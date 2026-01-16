using System;
using System.Collections.Generic;

using FluentAssertions;
using NUnit.Framework;
using RestSharp;

using HomeBudget.Accounting.Api.Constants;
using HomeBudget.Accounting.Api.IntegrationTests.WebApps;
using HomeBudget.Accounting.Domain.Models;
using HomeBudget.Components.Operations;

namespace HomeBudget.Accounting.Api.IntegrationTests.Api
{
    [TestFixture]
    [Category("Integration")]
    public class PaymentsHistoryControllerTests
    {
        private const string ApiHost = $"/{Endpoints.PaymentsHistory}";

        private readonly OperationsTestWebApp _sut = new();

        [Test]
        public void GetPaymentOperations_WhenTryToGetAllOperations_ThenIsSuccessStatusCode()
        {
            const string accountId = "92e8c2b2-97d9-4d6d-a9b7-48cb0d039a84";
            var getOperationsRequest = new RestRequest($"{ApiHost}/{accountId}");

            var response = _sut.RestHttpClient.Execute<Result<IReadOnlyCollection<PaymentOperationHistoryRecord>>>(getOperationsRequest);

            response.IsSuccessful.Should().BeTrue();
        }

        [Test]
        public void GetOperationById_WhenValidFilterById_ReturnsOperationWithExpectedAmount()
        {
            var operationId = Guid.Parse("2adb60a8-6367-4b8b-afa0-4ff7f7b1c92c");
            var accountId = Guid.Parse("92e8c2b2-97d9-4d6d-a9b7-48cb0d039a84");

            MockOperationsHistoryStore.SetState(new List<PaymentOperationHistoryRecord>
            {
                new()
                {
                    Record = new PaymentOperation
                    {
                        PaymentAccountId = accountId,
                        Key = operationId,
                        Amount = 35.64m
                    }
                }
            });

            var getOperationByIdRequest = new RestRequest($"{ApiHost}/{accountId}/byId/{operationId}");

            var response = _sut.RestHttpClient.Execute<Result<PaymentOperationHistoryRecord>>(getOperationByIdRequest);

            var result = response.Data;
            var payload = result.Payload;

            payload.Record.Amount.Should().Be(35.64m);
        }

        [Test]
        public void GetOperationById_WhenInValidFilterById_ReturnsFalseResult()
        {
            const string operationId = "invalid-operation-ref";
            const string accountId = "invalid-acc-ref";

            var getOperationByIdRequest = new RestRequest($"{ApiHost}/{accountId}/byId/{operationId}");

            var response = _sut.RestHttpClient.Execute<Result<PaymentOperationHistoryRecord>>(getOperationByIdRequest);

            var result = response.Data;

            result.IsSucceeded.Should().BeFalse();
        }
    }
}