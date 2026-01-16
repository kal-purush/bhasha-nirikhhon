// Copyright (c) All contributors. All rights reserved. Licensed under the MIT license.

using System.Diagnostics.CodeAnalysis;

namespace Netsphere;

/// <summary>
/// Represents a combination of a network connection and a service, providing functionality to manage their lifecycle.
/// </summary>
/// <remarks>This structure encapsulates a <see cref="Connection"/> and a <typeparamref name="TService"/>
/// instance, allowing for easy validation and disposal of the connection.<br/>
/// Use the <see cref="IsValid"/> property to check if both components are properly initialized, and <see cref="Dispose"/> to close the connection.</remarks>
/// <typeparam name="TService">The type of the service associated with the connection. Must implement <see cref="INetService"/>.</typeparam>
/// <param name="Connection">The network connection.</param>
/// <param name="Service">The service instance.</param>
public readonly record struct ConnectionAndService<TService>(Connection? Connection, TService? Service) : IDisposable
    where TService : INetService
{
    /// <summary>
    /// Gets a value indicating whether both the <see cref="Connection"/> and <see cref="Service"/> are valid (not null).
    /// </summary>
    /// <value><c>true</c> if both <see cref="Connection"/> and <see cref="Service"/> are not null; otherwise, <c>false</c>.</value>
    [MemberNotNullWhen(true, nameof(Connection))]
    [MemberNotNullWhen(true, nameof(Service))]
    public bool IsValid => this.Connection is not null && this.Service is not null;

    /// <summary>
    /// Gets a value indicating whether either the <see cref="Connection"/> or <see cref="Service"/> is invalid (null).
    /// </summary>
    /// <value><c>true</c> if either <see cref="Connection"/> or <see cref="Service"/> is null; otherwise, <c>false</c>.</value>
    [MemberNotNullWhen(false, nameof(Connection))]
    [MemberNotNullWhen(false, nameof(Service))]
    public bool IsInvalid => this.Connection is null || this.Service is null;

    /// <summary>
    /// Close the <see cref="Connection"/> if it is not null.
    /// </summary>
    public void Dispose()
    {
        this.Connection?.Dispose();
    }
}