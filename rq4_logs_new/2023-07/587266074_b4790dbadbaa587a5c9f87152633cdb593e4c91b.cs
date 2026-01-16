using Newtonsoft.Json;
using SelfService.Infrastructure.Api.Prometheus;

namespace SelfService.Tests.Infrastructure.Api;

public class TestPrometheusClientResultParsing
{
    [Fact]
    public async Task  parsing_failure_produces_empty_list()
    {
        // create dummy input
        string mock_response = @"{
            ""status"": ""failure""
        }";

        // define expectations
        var expected = new List<string>();

        // apply parser
        var actual = PrometheusClient.GetConsumersFromResponse(JsonConvert.DeserializeObject<Response>(mock_response), "topic");

        // verify input
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task  parsing_success_with_incomplete_data_produces_empty_list()
    {
        // create dummy input
        List<string> mock_responses = new List<string>();
        mock_responses.Append(@"{
            ""status"": ""success""
        }");
        mock_responses.Append(@"{
            ""status"": ""success"",
            ""data"": {
                ""resultType"": ""vector""
            }
        }");
        mock_responses.Append(@"{
            ""status"": ""success"",
            ""data"": {
                ""resultType"": ""vector"",
                ""result"": [
                    {
                        ""value"": [
                            123,
                            ""456""
                        ]
                    },
            }
        }");
        mock_responses.Append(@"{
            ""status"": ""success"",
            ""data"": {
                ""resultType"": ""vector"",
                ""result"": [
                    {
                        ""metric"": {
                            ""partition"": ""0""
                        },
                        ""value"": [
                            123,
                            ""456""
                        ]
                    },
            }
        }");

        // define expectations
        var expected = new List<string>();

        foreach (string mock in mock_responses) {
            var actual = PrometheusClient.GetConsumersFromResponse(JsonConvert.DeserializeObject<Response>(mock), "doesnotmatter");
            Assert.Equal(expected, actual);
        }
    }


    [Fact]
    public async Task  parsing_success_with_legal_result_returns_all_consumers_for_topic_across_partitions()
    {
        // create dummy input
        string mock_response = @"{
            ""status"": ""success"",
            ""data"": {
                ""resultType"": ""vector"",
                ""result"": [
                    {
                        ""metric"": {
                            ""consumergroup"": ""consumer1"",
                            ""topic"": ""topic"",
                            ""partition"": ""0""
                        },
                        ""value"": [
                            123,
                            ""456""
                        ]
                    },
                    {
                        ""metric"": {
                            ""consumergroup"": ""consumer2"",
                            ""topic"": ""topic"",
                            ""partition"": ""1""
                        },
                        ""value"": [
                            123,
                            ""456""
                        ]
                    },
                    {
                        ""metric"": {
                            ""consumergroup"": ""consumer3"",
                            ""topic"": ""topic2"",
                            ""partition"": ""0""
                        },
                        ""value"": [
                            123,
                            ""456""
                        ]
                    },
                ] 
            }
        }";

        // verification
        var expected = new List<string>{"consumer1", "consumer2"};
        var actual = PrometheusClient.GetConsumersFromResponse(JsonConvert.DeserializeObject<Response>(mock_response), "topic");
        Assert.Equal(expected, actual);

        var expected2 = new List<string>{"consumer3"};
        var actual2 = PrometheusClient.GetConsumersFromResponse(JsonConvert.DeserializeObject<Response>(mock_response), "topic2");
        Assert.Equal(expected2, actual2);
    }
}