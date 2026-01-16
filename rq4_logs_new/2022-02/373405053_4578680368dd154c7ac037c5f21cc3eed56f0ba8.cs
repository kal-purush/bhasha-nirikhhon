#nullable enable
using System;
using FluentEmail.Core.Interfaces;
using Lurgle.Alerting.Renderers;
using Microsoft.Extensions.DependencyInjection.Extensions;

// ReSharper disable UnusedType.Global
// ReSharper disable UnusedMember.Global

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>
    ///     FluentEmail Fluid/Liquid Builder Extensions
    /// </summary>
    public static class FluentEmailFluidBuilderExtensions
    {
        /// <summary>
        ///     Liquid Renderer
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="configure"></param>
        /// <returns></returns>
        public static FluentEmailServicesBuilder AddLiquidRenderer(
            this FluentEmailServicesBuilder builder,
            Action<LiquidRendererOptions>? configure = null)
        {
            builder.Services.AddOptions<LiquidRendererOptions>();
            if (configure != null) builder.Services.Configure(configure);

            builder.Services.TryAddSingleton<ITemplateRenderer, LiquidRenderer>();
            return builder;
        }
    }
}