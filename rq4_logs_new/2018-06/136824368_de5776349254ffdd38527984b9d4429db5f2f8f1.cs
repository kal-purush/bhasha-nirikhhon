using NUnit.Framework;
using Pipelines.Responses;

namespace Pipelines.Tests.Samples
{
    public class SampleTest
    {
        [Test]
        public void GivenValidOrder_WhenHandlingOrder_ThenOrderDispatchedIsReturned()
        {
            var pipeline = PipelineDefinitionBuilder<Order, Response<OrderDispatched, OrderError>>
                .StartWith(new Validation())
                .ThenWith(new OrderDispatchedHandler())
                .Build();

            var response = pipeline.Handle(new Order
            {
                Address = "32 north bridge",
                Amount = 20m,
                ItemNumber = "12234BDC",
                Name = "jordan"
            });

            Assert.That(response.Success, Is.TypeOf<OrderDispatched>());
        }

        [Test]
        public void GivenOrder_WithMissingAddress_WhenHandlingOrder_ThenReturnsOrderValidationError()
        {
            var pipeline = PipelineDefinitionBuilder<Order, Response<OrderDispatched, OrderError>>
                .StartWith(new Validation())
                .ThenWith(new OrderDispatchedHandler())
                .Build();

            var response = pipeline.Handle(new Order());

            Assert.That(response.Error, Is.EqualTo(OrderError.Validation));
        }
    }
}