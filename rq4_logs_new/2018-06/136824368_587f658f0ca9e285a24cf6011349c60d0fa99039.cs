using System;
using System.Collections.Generic;
using System.Linq;
using Anderson.Pipelines.Builders;
using NUnit.Framework;

namespace Anderson.Pipelines.Tests.Pipelines
{
    public class PipelineTestWithResolver
    {
        [Test]
        public void GivenPiplelineWithThreeStages_WhenHandlingRequest_ThenAllThreeHandlersHandleRequest()
        {
            var testHandlerA = new TestHandlerA<TestRequestA>();
            var testHandlerB = new TestHandlerB<TestRequestA>();
            var testHandlerC = new TestHandlerC<TestRequestA>();

            Dictionary<Type, Object> dictionary = new Dictionary<Type, object>
            {
                [typeof(TestHandlerA<TestRequestA>)] = testHandlerA,
                [typeof(TestHandlerB<TestRequestA>)] = testHandlerB,
                [typeof(TestHandlerC<TestRequestA>)] = testHandlerC,
            };

            PipelineDefinitionBuilder<TestRequestA, TestResponse> builder = new PipelineDefinitionBuilder<TestRequestA, TestResponse>(type => dictionary[type]);
            
            var pipeline = builder.StartWith<TestHandlerA<TestRequestA>>()
                                  .ThenWith<TestHandlerB<TestRequestA>>()
                                  .ThenWith<TestHandlerC<TestRequestA>>()
                                  .Build();

            var testRequest = new TestRequestA();
            pipeline.Handle(testRequest);

            Assert.Multiple(() =>
            {
                Assert.That(testHandlerA._request, Is.EqualTo(testRequest));
                Assert.That(testHandlerB._request, Is.EqualTo(testRequest));
                Assert.That(testHandlerC._request, Is.EqualTo(testRequest));
            });
        }

        [Test]
        public void GivenPiplelineWithThreeStages_WhenHandlingRequest_ThenAllThreeHandlersAreCalledInOrder()
        {
            var testHandlerA = new TestHandlerA<TestRequestA>();
            var testHandlerB = new TestHandlerB<TestRequestA>();
            var testHandlerC = new TestHandlerC<TestRequestA>();

            Dictionary<Type, Object> dictionary = new Dictionary<Type, object>
            {
                [typeof(TestHandlerA<TestRequestA>)] = testHandlerA,
                [typeof(TestHandlerB<TestRequestA>)] = testHandlerB,
                [typeof(TestHandlerC<TestRequestA>)] = testHandlerC,
            };

            PipelineDefinitionBuilder<TestRequestA, TestResponse> builder = new
                PipelineDefinitionBuilder<TestRequestA, TestResponse>(type => dictionary[type]);

            var pipeline = builder.StartWith<TestHandlerA<TestRequestA>>()
                .ThenWith<TestHandlerB<TestRequestA>>()
                .ThenWith<TestHandlerC<TestRequestA>>()
                .Build();

            var testRequest = new TestRequestA();

            pipeline.Handle(testRequest);

            var callOrder = new TestHandler<TestRequestA>[]
                {
                    testHandlerA,
                    testHandlerB,
                    testHandlerC
                }.OrderBy(x => x._timestamp)
                .ToArray();

            Assert.Multiple(() =>
            {
                Assert.That(callOrder[0], Is.EqualTo(testHandlerA));
                Assert.That(callOrder[1], Is.EqualTo(testHandlerB));
                Assert.That(callOrder[2], Is.EqualTo(testHandlerC));
            });
        }

        [Test]
        public void GivenPipelineThatChangesRequest_WhenHandlingRequest_ThenChangesRequestInPipeline()
        {
            var testHandlerA = new TestHandlerA<TestRequestA>();
            var testHandlerB = new TestHandlerB<TestRequestA>();
            var testMutationHandler = new TestMutationHandler();
            var testHandlerC = new TestHandlerC<TestRequestB>();

            Dictionary<Type, Object> dictionary = new Dictionary<Type, object>
            {
                [typeof(TestHandlerA<TestRequestA>)] = testHandlerA,
                [typeof(TestHandlerB<TestRequestA>)] = testHandlerB,
                [typeof(TestHandlerC<TestRequestB>)] = testHandlerC,
                [typeof(TestMutationHandler)] = testMutationHandler,
            };

            PipelineDefinitionBuilder<TestRequestA, TestResponse> builder = new
                PipelineDefinitionBuilder<TestRequestA, TestResponse>(type => dictionary[type]);


            var pipeline = builder.StartWith<TestHandlerA<TestRequestA>>()
                .ThenWith<TestHandlerB<TestRequestA>>()
                .ThenWithMutation<TestMutationHandler, TestRequestB>()
                .ThenWith<TestHandlerC<TestRequestB>>()
                .Build();

            var testRequest = new TestRequestA();

            pipeline.Handle(testRequest);

            Assert.Multiple(() =>
            {
                Assert.That(testHandlerA._request, Is.EqualTo(testRequest));
                Assert.That(testHandlerB._request, Is.EqualTo(testRequest));
                Assert.That(testMutationHandler._request, Is.EqualTo(testRequest));
                Assert.That(testHandlerC._request, Is.Not.EqualTo(testRequest));
                Assert.That(testHandlerC._request, Is.TypeOf<TestRequestB>());
            });
        }
    }
}