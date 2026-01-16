using System.Net.Http.Json;
using System.Text.Json;
using ClientNexus.Domain.Exceptions.ServerErrorsExceptions;
using ClientNexus.Domain.Interfaces;

namespace ClientNexus.Infrastructure
{
    internal class ExpoErrorResponse
    {
        public List<ExpoErrorResult>? errors { get; set; }
    }

    internal class ExpoErrorResult
    {
        public string? code { get; set; }
        public string? message { get; set; }
    }

    internal class ExpoPushResponseFailure
    {
        public List<ExpoPushResultFailure>? data { get; set; }
    }

    internal class ExpoPushResultFailure
    {
        public string? status { get; set; }
        public string? message { get; set; }
        public ExpoErrorDetails? details { get; set; }
    }

    internal class ExpoErrorDetails
    {
        public string? error { get; set; }
    }

    internal class ExpoPushResponseSuccess
    {
        public ExpoPushResultSuccess? data { get; set; }
    }

    internal class ExpoPushResultSuccess
    {
        public string? status { get; set; }
        public string? id { get; set; } = null;
    }

    public class ExpoPushNotification : IPushNotification
    {
        private readonly HttpClient _httpClient;

        public ExpoPushNotification()
        {
            _httpClient = new HttpClient(
                new HttpClientHandler
                {
                    AutomaticDecompression =
                        System.Net.DecompressionMethods.GZip
                        | System.Net.DecompressionMethods.Deflate,
                }
            );

            _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            _httpClient.DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate");
        }

        public async Task<string> SendNotificationAsync(
            string title,
            string body,
            string deviceToken,
            Dictionary<string, string>? data = null
        )
        {
            HttpResponseMessage response;
            if (data is not null)
            {
                response = await _httpClient.PostAsync(
                    "https://exp.host/--/api/v2/push/send",
                    JsonContent.Create(
                        new
                        {
                            to = deviceToken,
                            title = title,
                            body = body,
                            data = data,
                        }
                    )
                );
            }
            else
            {
                response = await _httpClient.PostAsync(
                    "https://exp.host/--/api/v2/push/send",
                    JsonContent.Create(
                        new
                        {
                            to = deviceToken,
                            title = title,
                            body = body,
                        }
                    )
                );
            }

            if (!response.IsSuccessStatusCode)
            {
                var errorResponse = await response.Content.ReadFromJsonAsync<ExpoErrorResponse>();
                throw new ServerException(errorResponse!.errors[0]!.message);
            }

            var strResponse = await response.Content.ReadAsStringAsync();
            Console.WriteLine(strResponse);

            try
            {
                var pushResponseSuccess = JsonSerializer.Deserialize<ExpoPushResponseSuccess>(
                    strResponse
                );
                if (pushResponseSuccess is not null)
                {
                    return pushResponseSuccess.data!.id!;
                }
            }
            catch (JsonException) { }

            var pushResponseFailure = JsonSerializer.Deserialize<ExpoPushResponseFailure>(
                strResponse
            );
            if (pushResponseFailure is null)
            {
                Console.WriteLine("pushResponse is null");
                throw new ServerException("Error in the code of notification service 1");
            }

            if (pushResponseFailure.data is null)
            {
                Console.WriteLine("pushResponse data is null");
                throw new ServerException("Error in the code of notification service 2");
            }

            var result = pushResponseFailure.data[0];
            if (result.status == "error")
            {
                if (result.details?.error == "DeviceNotRegistered")
                {
                    throw new DeviceNotRegisteredException("Device not registered");
                }
                else if (result.details?.error == "MessageRateExceeded")
                {
                    throw new MessageRateExceeded("Message rate exceeded");
                }
                else
                {
                    Console.WriteLine($"Unknown error: {result.message}");
                    throw new PushNotificationException(result.message);
                }
            }

            Console.WriteLine($"Unknown error: {result.message}");
            throw new PushNotificationException(result.message);
        }
    }
}