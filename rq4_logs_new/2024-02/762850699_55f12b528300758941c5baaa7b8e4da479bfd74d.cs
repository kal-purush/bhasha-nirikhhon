using System;
using System.Threading.Tasks;
using Twilio.Clients;

namespace Soenneker.Twilio.RestClient.Abstract;

/// <summary>
/// An async thread-safe singleton for a Twilio RestClient
/// </summary>
public interface ITwilioRestClientUtil : IDisposable, IAsyncDisposable
{
    ValueTask<TwilioRestClient> Get();
}