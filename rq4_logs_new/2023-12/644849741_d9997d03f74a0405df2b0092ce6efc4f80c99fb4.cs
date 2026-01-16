// Copyright (c) Aksio Insurtech. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Reflection;
using Castle.DynamicProxy;
using MongoDB.Driver;
using Polly;

namespace Aksio.MongoDB;

/// <summary>
/// Represents a selector for <see cref="MongoCollectionInterceptor"/>.
/// </summary>
public class MongoCollectionInterceptorSelector : IInterceptorSelector
{
    readonly ResiliencePipeline _resiliencePipeline;
    readonly IMongoClient _mongoClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoCollectionInterceptorSelector"/> class.
    /// </summary>
    /// <param name="resiliencePipeline">The <see cref="ResiliencePipeline"/> to use.</param>
    /// <param name="mongoClient"><see cref="IMongoClient"/> to intercept.</param>
    public MongoCollectionInterceptorSelector(
        ResiliencePipeline resiliencePipeline,
        IMongoClient mongoClient)
    {
        _resiliencePipeline = resiliencePipeline;
        _mongoClient = mongoClient;
    }

    /// <inheritdoc/>
    public IInterceptor[] SelectInterceptors(Type type, MethodInfo method, IInterceptor[] interceptors)
    {
        if (method.ReturnType == typeof(Task))
        {
            return new[] { new MongoCollectionInterceptor(_resiliencePipeline, _mongoClient) };
        }
        return Array.Empty<IInterceptor>();
    }
}