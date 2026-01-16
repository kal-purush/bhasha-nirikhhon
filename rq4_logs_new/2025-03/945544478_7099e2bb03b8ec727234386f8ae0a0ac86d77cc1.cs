using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.NameCom.Client.Abstract;

/// <summary>
/// A .NET thread-safe singleton HttpClient for Name.com
/// </summary>
public interface INameComClientUtil : IDisposable, IAsyncDisposable
{
    ValueTask<HttpClient> Get(CancellationToken cancellationToken = default);
}