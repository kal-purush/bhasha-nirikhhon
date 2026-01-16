using Microsoft.AspNetCore.Mvc;
using Mortein.Mqtt.Services;
using Mortein.Types;
using MQTTnet.Client;
using NodaTime.Serialization.SystemTextJson;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Mortein.Controllers;


/// <summary>
/// API controller for device commands.
/// </summary>
/// 
/// <param name="context">The context which enables interaction with the database.</param>
/// <param name="clientService">The service exposing the client which enables publishing to the MQTT client.</param>
[ApiController]
[Route("Device/{deviceId}/[controller]")]
public class CommandController(DatabaseContext context, MqttClientService clientService) : ControllerBase
{
    /// The context which enables interaction with the database.
    private readonly DatabaseContext _context = context;

    /// The client which enables publishing to the MQTT client.
    private readonly IMqttClient _client = clientService.MqttClient;

    private static readonly JsonSerializerOptions jsonSerializerOptions = new()
    {
        Converters =
        {
            NodaConverters.InstantConverter,
            new JsonStringEnumConverter()
        }
    };

    /// <summary>
    /// Publish a command to a device.
    /// </summary>
    /// 
    /// <param name="deviceId">The device to which to publish a command.</param>
    /// <param name="command">The command to publish.</param>
    private async void PublishCommand(Guid deviceId, Command command)
    {
        await _client.PublishStringAsync(ConstructTopicName(deviceId), JsonSerializer.Serialize(command, jsonSerializerOptions));
    }

    /// <summary>
    /// Construct a topic name for a device
    /// </summary>
    /// <param name="deviceId">The device for which to construct a topic name.</param>
    /// <returns></returns>
    private static string ConstructTopicName(Guid deviceId)
    {
        return deviceId.ToString();
    }
}